//! MCP Protocol Smoke Test
//!
//! Ensures the MCP server is functional at the protocol level by:
//! - Testing server initialization
//! - Verifying tools, prompts, and resources are advertised
//! - Running one end-to-end tool invocation
//!
//! This is NOT exhaustive testing - just enough to catch protocol-level breakage.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Helper struct to manage MCP server process and communicate via stdio
struct McpClient {
    process: Child,
    request_id: u64,
}

impl McpClient {
    /// Spawn MCP server as subprocess with stdio transport
    fn spawn() -> Self {
        let endpoint = std::env::var("DANUBE_ADMIN_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

        let process = Command::new(assert_cmd::cargo::cargo_bin!("danube-admin"))
            .arg("serve")
            .arg("--mode")
            .arg("mcp")
            .env("DANUBE_ADMIN_ENDPOINT", endpoint)
            .env("DANUBE_BROKER_ENDPOINT", "http://127.0.0.1:6650")
            .env("RUST_LOG", "error") // Reduce noise in test output
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn MCP server");

        // Give server a moment to start
        std::thread::sleep(Duration::from_millis(500));

        Self {
            process,
            request_id: 0,
        }
    }

    /// Send JSON-RPC request and read response
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.request_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params
        });

        let request_str = serde_json::to_string(&request).unwrap();
        let stdin = self.process.stdin.as_mut().expect("stdin not captured");

        writeln!(stdin, "{}", request_str).expect("Failed to write to stdin");
        stdin.flush().expect("Failed to flush stdin");

        // Read response from stdout - read until we get valid JSON
        let stdout = self.process.stdout.as_mut().expect("stdout not captured");
        let mut reader = BufReader::new(stdout);

        // Try to read lines until we get a valid JSON-RPC response
        // (skip any log lines or non-JSON output)
        for _ in 0..10 {
            let mut response_line = String::new();
            match reader.read_line(&mut response_line) {
                Ok(0) => panic!("EOF: MCP server closed stdout"),
                Ok(_) => {
                    let trimmed = response_line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    // Try to parse as JSON-RPC response
                    if let Ok(json) = serde_json::from_str::<Value>(trimmed) {
                        if json.get("jsonrpc").is_some()
                            || json.get("result").is_some()
                            || json.get("error").is_some()
                        {
                            return json;
                        }
                    }
                    // Not a valid JSON-RPC response, might be a log line
                    eprintln!("Skipping non-JSON line: {}", trimmed);
                }
                Err(e) => panic!("Failed to read response: {}", e),
            }
        }

        panic!("Failed to read valid JSON-RPC response after 10 attempts")
    }

    /// Initialize the MCP server
    fn initialize(&mut self) -> Value {
        let response = self.request(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "roots": {
                        "listChanged": false
                    }
                },
                "clientInfo": {
                    "name": "mcp-smoke-test",
                    "version": "1.0.0"
                }
            }),
        );

        // Send initialized notification (required by MCP protocol)
        self.send_notification("notifications/initialized", json!({}));

        response
    }

    /// Send a notification (no response expected)
    fn send_notification(&mut self, method: &str, params: Value) {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });

        let notification_str = serde_json::to_string(&notification).unwrap();
        let stdin = self.process.stdin.as_mut().expect("stdin not captured");

        writeln!(stdin, "{}", notification_str).expect("Failed to write notification");
        stdin.flush().expect("Failed to flush stdin");

        // Give server a moment to process notification
        std::thread::sleep(Duration::from_millis(100));
    }

    /// List available tools
    fn list_tools(&mut self) -> Value {
        self.request("tools/list", json!({}))
    }

    /// List available prompts
    fn list_prompts(&mut self) -> Value {
        self.request("prompts/list", json!({}))
    }

    /// List available resources
    fn list_resources(&mut self) -> Value {
        self.request("resources/list", json!({}))
    }

    /// Call a tool
    fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.request(
            "tools/call",
            json!({
                "name": name,
                "arguments": arguments
            }),
        )
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[test]
fn mcp_server_initialization_and_capabilities() {
    let mut client = McpClient::spawn();

    // Test initialization
    let response = client.initialize();

    // Verify response structure
    assert!(
        response.get("result").is_some(),
        "Missing result in initialize response"
    );
    let result = &response["result"];

    // Verify server info
    assert!(result.get("serverInfo").is_some(), "Missing serverInfo");
    let server_info = &result["serverInfo"];
    // Server name comes from rmcp library, just verify it exists
    assert!(server_info.get("name").is_some(), "Missing server name");

    // Verify capabilities
    assert!(result.get("capabilities").is_some(), "Missing capabilities");
    let capabilities = &result["capabilities"];

    // Verify tools, prompts, and resources are advertised
    assert!(
        capabilities.get("tools").is_some(),
        "Tools capability not advertised"
    );
    assert!(
        capabilities.get("prompts").is_some(),
        "Prompts capability not advertised"
    );
    assert!(
        capabilities.get("resources").is_some(),
        "Resources capability not advertised"
    );

    println!("✅ Server initialized successfully with all capabilities");
}

#[test]
fn mcp_tools_discovery() {
    let mut client = McpClient::spawn();
    client.initialize();

    // List tools
    let response = client.list_tools();
    assert!(
        response.get("result").is_some(),
        "Missing result in list_tools response"
    );

    let result = &response["result"];
    let tools = result["tools"]
        .as_array()
        .expect("tools should be an array");

    // Verify we have a substantial number of tools (at least 20)
    assert!(
        tools.len() >= 20,
        "Expected at least 20 tools, found {}. Full list: {:?}",
        tools.len(),
        tools.iter().map(|t| t["name"].as_str()).collect::<Vec<_>>()
    );

    // Verify each tool has required fields
    for tool in tools {
        assert!(tool.get("name").is_some(), "Tool missing name: {:?}", tool);
        assert!(
            tool.get("description").is_some(),
            "Tool missing description: {:?}",
            tool
        );
        assert!(
            tool.get("inputSchema").is_some(),
            "Tool missing inputSchema: {:?}",
            tool
        );
    }

    println!("✅ Discovered {} tools", tools.len());
}

#[test]
fn mcp_prompts_discovery() {
    let mut client = McpClient::spawn();
    client.initialize();

    // List prompts
    let response = client.list_prompts();
    assert!(
        response.get("result").is_some(),
        "Missing result in list_prompts response"
    );

    let result = &response["result"];
    let prompts = result["prompts"]
        .as_array()
        .expect("prompts should be an array");

    // Verify we have at least some prompts
    assert!(
        !prompts.is_empty(),
        "Expected at least one prompt to be available"
    );

    // Verify each prompt has required fields
    for prompt in prompts {
        assert!(
            prompt.get("name").is_some(),
            "Prompt missing name: {:?}",
            prompt
        );
        assert!(
            prompt.get("description").is_some(),
            "Prompt missing description: {:?}",
            prompt
        );
    }

    println!("✅ Discovered {} prompts", prompts.len());
}

#[test]
fn mcp_resources_discovery() {
    let mut client = McpClient::spawn();
    client.initialize();

    // List resources
    let response = client.list_resources();
    assert!(
        response.get("result").is_some(),
        "Missing result in list_resources response"
    );

    let result = &response["result"];
    let resources = result["resources"]
        .as_array()
        .expect("resources should be an array");

    // Verify we have at least some resources
    assert!(
        !resources.is_empty(),
        "Expected at least one resource to be available"
    );

    // Verify each resource has required fields
    for resource in resources {
        assert!(
            resource.get("uri").is_some(),
            "Resource missing uri: {:?}",
            resource
        );
        assert!(
            resource.get("name").is_some(),
            "Resource missing name: {:?}",
            resource
        );
    }

    println!("✅ Discovered {} resources", resources.len());
}

#[test]
fn mcp_tool_invocation_end_to_end() {
    let mut client = McpClient::spawn();
    client.initialize();

    // Call a simple tool that doesn't require complex setup (list_brokers)
    let response = client.call_tool("list_brokers", json!({}));

    // Verify response structure
    assert!(
        response.get("result").is_some(),
        "Missing result in tool call response"
    );
    let result = &response["result"];

    // Verify content is returned
    assert!(
        result.get("content").is_some(),
        "Missing content in tool response"
    );
    let content = result["content"]
        .as_array()
        .expect("content should be an array");
    assert!(!content.is_empty(), "Tool returned empty content");

    // Verify content has text type
    let first_content = &content[0];
    assert_eq!(first_content["type"], "text", "Expected text content type");
    assert!(
        first_content.get("text").is_some(),
        "Missing text field in content"
    );

    let text = first_content["text"].as_str().unwrap();
    println!(
        "✅ Tool invocation successful. Sample output:\n{}",
        &text.chars().take(200).collect::<String>()
    );
}

#[test]
fn mcp_tool_descriptions_from_doc_comments() {
    let mut client = McpClient::spawn();
    client.initialize();

    // List tools and verify specific descriptions are properly extracted from /// comments
    let response = client.list_tools();
    let tools = response["result"]["tools"]
        .as_array()
        .expect("tools should be an array");

    // Helper to find tool by name
    let find_tool = |name: &str| {
        tools
            .iter()
            .find(|t| t["name"].as_str() == Some(name))
            .expect(&format!("Tool '{}' not found", name))
    };

    // Test 1: Verify list_brokers has comprehensive description
    let list_brokers = find_tool("list_brokers");
    let desc = list_brokers["description"].as_str().unwrap();
    assert!(
        desc.contains("Returns broker IDs, status"),
        "list_brokers description missing expected content. Got: {}",
        desc
    );
    assert!(
        desc.contains("Use this first to discover"),
        "list_brokers description missing usage guidance. Got: {}",
        desc
    );

    // Test 2: Verify get_broker_logs mentions config requirement
    let get_logs = find_tool("get_broker_logs");
    let desc = get_logs["description"].as_str().unwrap();
    assert!(
        desc.contains("Config Required") || desc.contains("mcp-config.yml"),
        "get_broker_logs should mention config requirement. Got: {}",
        desc
    );

    // Test 3: Verify get_cluster_metrics mentions Prometheus
    let cluster_metrics = find_tool("get_cluster_metrics");
    let desc = cluster_metrics["description"].as_str().unwrap();
    assert!(
        desc.contains("Prometheus"),
        "get_cluster_metrics should mention Prometheus requirement. Got: {}",
        desc
    );

    // Test 4: Verify inputSchema is present and valid
    let create_topic = find_tool("create_topic");
    let schema = &create_topic["inputSchema"];
    assert!(
        schema.get("type").is_some(),
        "inputSchema should have a type field"
    );
    assert!(
        schema.get("properties").is_some(),
        "inputSchema should have properties for parameters"
    );

    // Verify parameter descriptions are included (from JsonSchema derive)
    let properties = schema["properties"].as_object().unwrap();
    if let Some(name_field) = properties.get("name") {
        assert!(
            name_field.get("description").is_some(),
            "Parameter 'name' should have description from doc comment"
        );
    }
}

#[test]
fn mcp_error_handling() {
    let mut client = McpClient::spawn();
    client.initialize();

    // Try to call a non-existent tool
    let response = client.call_tool("nonexistent_tool", json!({}));

    // Verify error response
    assert!(
        response.get("error").is_some(),
        "Expected error for non-existent tool, got: {:?}",
        response
    );

    println!("✅ Error handling works correctly");
}
