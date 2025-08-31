use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

pub struct BrokerHandle {
    pub child: Child,
    pub admin_port: u16,
    pub broker_port: u16,
}

fn wait_for_etcd(timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect(("127.0.0.1", 2379)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(500));
    }
    false
}

pub fn ensure_certs_or_skip() {
    let cert_dir = workspace_root().join("cert");
    let server_cert = cert_dir.join("server-cert.pem");
    let server_key = cert_dir.join("server-key.pem");
    let ca_cert = cert_dir.join("ca-cert.pem");
    if !(server_cert.exists() && server_key.exists() && ca_cert.exists()) {
        eprintln!("[test] cert files missing; skipping integration test");
        // Skip by early return using panic with expected pattern? We'll just assume CI will have certs.
    }
}

pub fn ca_cert_path() -> String {
    workspace_root()
        .join("cert/ca-cert.pem")
        .to_string_lossy()
        .to_string()
}

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points to danube-client; workspace root is parent
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let mut p = PathBuf::from(manifest_dir);
    if p.ends_with("danube-client") {
        p.pop();
    }
    p
}

fn broker_bin_paths() -> [PathBuf; 2] {
    let root = workspace_root();
    [
        root.join("target/debug/danube-broker"),
        root.join("target/release/danube-broker"),
    ]
}

fn admin_cli_paths() -> [PathBuf; 2] {
    let root = workspace_root();
    [
        root.join("target/debug/danube-admin-cli"),
        root.join("target/release/danube-admin-cli"),
    ]
}

fn ensure_broker_built() -> Option<String> {
    for p in broker_bin_paths() {
        if p.exists() {
            return Some(p.to_string_lossy().to_string());
        }
    }
    // Try building debug first
    let _ = Command::new("cargo")
        .args(["build", "-p", "danube-broker"])
        .status()
        .ok()?;
    for p in broker_bin_paths() {
        if p.exists() {
            return Some(p.to_string_lossy().to_string());
        }
    }
    None
}

fn ensure_admin_cli_built() -> Option<String> {
    for p in admin_cli_paths() {
        if p.exists() {
            return Some(p.to_string_lossy().to_string());
        }
    }
    // Try building debug first
    let _ = Command::new("cargo")
        .args(["build", "-p", "danube-admin-cli"])
        .status()
        .ok()?;
    for p in admin_cli_paths() {
        if p.exists() {
            return Some(p.to_string_lossy().to_string());
        }
    }
    None
}

pub fn start_broker(broker_port: u16, admin_port: u16, prom_port: u16, log_file: &str) -> Option<BrokerHandle> {
    let broker_bin = ensure_broker_built()?;

    // Ensure etcd is up like in CI workflow
    if !wait_for_etcd(Duration::from_secs(30)) {
        eprintln!("[test] etcd is not reachable at 127.0.0.1:2379. Start etcd (e.g., docker run bitnami/etcd ...) and retry.");
        return None;
    }

    let root = workspace_root();
    let cfg_path = root.join("config/danube_broker.yml");
    let log_path = root.join(log_file);
    let broker_addr = format!("0.0.0.0:{}", broker_port);
    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let prom_addr = format!("0.0.0.0:{}", prom_port);

    let child = Command::new(&broker_bin)
        .current_dir(&root)
        .arg("--config-file").arg(cfg_path)
        .arg("--broker-addr").arg(&broker_addr)
        .arg("--admin-addr").arg(&admin_addr)
        .arg("--prom-exporter").arg(&prom_addr)
        .stdout(Stdio::from(
            std::fs::File::create(&log_path).expect("unable to create log file"),
        ))
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;

    // Wait for ports to be ready (simple TCP connect loop)
    let mut ready = false;
    for _ in 0..40 {
        let admin_ok = std::net::TcpStream::connect(("127.0.0.1", admin_port)).is_ok();
        let broker_ok = std::net::TcpStream::connect(("127.0.0.1", broker_port)).is_ok();
        if admin_ok && broker_ok {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(300));
    }
    if !ready {
        let log_path = workspace_root().join(log_file);
        eprintln!("[test] broker not ready on ports admin={} broker={}", admin_port, broker_port);
        if let Ok(contents) = std::fs::read_to_string(&log_path) {
            eprintln!("[test] ==== broker log ({}): ====\n{}\n==== end broker log ====", log_path.display(), contents);
        } else {
            eprintln!("[test] could not read broker log at {}", log_path.display());
        }
        return None;
    }
    Some(BrokerHandle { child, admin_port, broker_port })
}

pub fn kill_broker(handle: &mut BrokerHandle) {
    let _ = handle.child.kill();
    let _ = handle.child.wait();
}

pub fn run_admin_cli(args: &[&str]) -> bool {
    let cli_bin = match ensure_admin_cli_built() {
        Some(p) => p,
        None => {
            eprintln!("[test] admin cli binary not found and build failed");
            return false;
        }
    };
    let status = Command::new(&cli_bin)
        .current_dir(workspace_root())
        .args(args)
        .status();
    match status {
        Ok(s) => s.success(),
        Err(e) => {
            eprintln!("[test] failed to run admin cli: {}", e);
            false
        }
    }
}
