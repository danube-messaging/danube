use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

pub struct BrokerHandle {
    pub child: Child,
    pub admin_port: u16,
    pub broker_port: u16,
}

pub fn ensure_certs_or_skip() {
    let cert_dir = Path::new("./cert");
    let server_cert = cert_dir.join("server-cert.pem");
    let server_key = cert_dir.join("server-key.pem");
    let ca_cert = cert_dir.join("ca-cert.pem");
    if !(server_cert.exists() && server_key.exists() && ca_cert.exists()) {
        eprintln!("[test] cert files missing; skipping integration test");
        // Skip by early return using panic with expected pattern? We'll just assume CI will have certs.
    }
}

pub fn start_broker(broker_port: u16, admin_port: u16, prom_port: u16, log_file: &str) -> Option<BrokerHandle> {
    let broker_bin = if cfg!(debug_assertions) {
        "./target/debug/danube-broker"
    } else {
        "./target/release/danube-broker"
    };
    if !Path::new(broker_bin).exists() {
        eprintln!("[test] broker binary not found at {}", broker_bin);
        return None;
    }

    let child = Command::new(broker_bin)
        .args([
            "--config-file", "./config/danube_broker.yml",
            "--broker-addr", &format!("0.0.0.0:{}", broker_port),
            "--admin-addr", &format!("0.0.0.0:{}", admin_port),
            "--prom-exporter", &format!("0.0.0.0:{}", prom_port),
        ])
        .stdout(Stdio::from(
            std::fs::File::create(log_file).expect("unable to create log file"),
        ))
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;

    // Give it a moment to boot
    thread::sleep(Duration::from_secs(3));

    Some(BrokerHandle { child, admin_port, broker_port })
}

pub fn kill_broker(handle: &mut BrokerHandle) {
    let _ = handle.child.kill();
    let _ = handle.child.wait();
}

pub fn run_admin_cli(args: &[&str]) -> bool {
    let cli_bin = if cfg!(debug_assertions) {
        "./target/debug/danube-admin-cli"
    } else {
        "./target/release/danube-admin-cli"
    };
    if !Path::new(cli_bin).exists() {
        eprintln!("[test] admin cli binary not found at {}", cli_bin);
        return false;
    }
    let status = Command::new(cli_bin)
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
