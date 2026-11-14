use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use danube_core::jwt::{create_token, Claims};
use danube_core::metadata::{MetaOptions, MetadataStore};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::digest::{digest, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::security::config::{JwtConfig, TlsConfig};

pub(crate) const CLUSTER_CA_PATH: &str = "/cluster/security/ca";
pub(crate) const BOOTSTRAP_ADMIN_SUBJECT: &str = "bootstrap-admin";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterCaRecord {
    pub(crate) ca_cert_pem: String,
    pub(crate) encrypted_ca_key_b64: String,
    pub(crate) nonce_b64: String,
    pub(crate) jwt_secret_key: String,
    pub(crate) created_at_secs: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AutoTlsProvisioned {
    pub(crate) tls: TlsConfig,
    pub(crate) jwt: JwtConfig,
    pub(crate) ca_cert_pem: String,
    pub(crate) ca_key_pem: Option<String>,
    pub(crate) bootstrap_token_path: PathBuf,
}

fn openssl_bin() -> &'static str {
    if Path::new("/usr/bin/openssl").exists() {
        "/usr/bin/openssl"
    } else {
        "openssl"
    }
}

fn set_owner_only_perms(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed to set 0600 permissions on {}", path.display()))?;
    }
    Ok(())
}

/// Derive or persist a 32-byte encryption key for encrypting the Cluster CA key at rest.
/// Uses `DANUBE_CLUSTER_SECRET` if set, otherwise reads/generates `{data_dir}/cluster_ca.secret`.
pub(crate) fn load_or_create_cluster_secret(data_dir: &Path) -> Result<[u8; 32]> {
    if let Ok(env_secret) = std::env::var("DANUBE_CLUSTER_SECRET") {
        let trimmed = env_secret.trim();
        if !trimmed.is_empty() {
            let d = digest(&SHA256, trimmed.as_bytes());
            let mut key = [0u8; 32];
            key.copy_from_slice(d.as_ref());
            return Ok(key);
        }
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data_dir {}", data_dir.display()))?;
    let secret_path = data_dir.join("cluster_ca.secret");
    if secret_path.exists() {
        let existing = fs::read_to_string(&secret_path)
            .with_context(|| format!("failed to read {}", secret_path.display()))?;
        let d = digest(&SHA256, existing.trim().as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(d.as_ref());
        return Ok(key);
    }

    let mut raw = [0u8; 32];
    SystemRandom::new()
        .fill(&mut raw)
        .map_err(|_| anyhow::anyhow!("failed to generate cluster CA secret"))?;
    let encoded = B64.encode(raw);
    fs::write(&secret_path, format!("{encoded}\n"))
        .with_context(|| format!("failed to write {}", secret_path.display()))?;
    set_owner_only_perms(&secret_path)?;

    let d = digest(&SHA256, encoded.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(d.as_ref());
    Ok(key)
}

pub(crate) fn encrypt_ca_key(ca_key_pem: &[u8], key: &[u8; 32]) -> Result<(String, String)> {
    let unbound = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|_| anyhow::anyhow!("invalid AES-256-GCM key"))?;
    let sealing_key = LessSafeKey::new(unbound);

    let mut nonce_bytes = [0u8; 12];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate AEAD nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = ca_key_pem.to_vec();
    sealing_key
        .seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("AES-256-GCM encryption failed"))?;

    Ok((B64.encode(nonce_bytes), B64.encode(in_out)))
}

pub(crate) fn decrypt_ca_key(
    nonce_b64: &str,
    ciphertext_b64: &str,
    key: &[u8; 32],
) -> Result<Vec<u8>> {
    let nonce_bytes = B64.decode(nonce_b64).context("invalid nonce base64")?;
    let nonce_arr: [u8; 12] = nonce_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid AES-256-GCM nonce length"))?;

    let mut in_out = B64
        .decode(ciphertext_b64)
        .context("invalid ciphertext base64")?;

    let unbound = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|_| anyhow::anyhow!("invalid AES-256-GCM key"))?;
    let opening_key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(nonce_arr);
    let plaintext = opening_key
        .open_in_place(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to decrypt cluster CA private key"))?;

    Ok(plaintext.to_vec())
}

fn run_openssl(args: &[&str]) -> Result<()> {
    let out = Command::new(openssl_bin())
        .args(args)
        .output()
        .context("failed to invoke openssl binary")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!("openssl {:?} failed: {}", args, stderr.trim());
    }
    Ok(())
}

fn generate_ecdsa_ca(ca_cert_path: &Path, ca_key_path: &Path) -> Result<()> {
    let ca_key_str = ca_key_path.to_str().context("non-utf8 ca key path")?;
    let ca_cert_str = ca_cert_path.to_str().context("non-utf8 ca cert path")?;

    run_openssl(&["ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", ca_key_str])?;
    set_owner_only_perms(ca_key_path)?;
    run_openssl(&[
        "req",
        "-new",
        "-x509",
        "-key",
        ca_key_str,
        "-out",
        ca_cert_str,
        "-days",
        "3650",
        "-subj",
        "/CN=danube-cluster-ca",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    ])?;
    Ok(())
}

fn sign_leaf_cert(
    ca_cert_path: &Path,
    ca_key_path: &Path,
    leaf_cert_path: &Path,
    leaf_key_path: &Path,
    san_hosts: &[String],
) -> Result<()> {
    let dir = leaf_cert_path
        .parent()
        .context("leaf cert path missing parent")?;
    let csr_path = dir.join("node.csr");
    let ext_path = dir.join("node.ext");
    let srl_path = dir.join("ca.srl");

    let leaf_key_str = leaf_key_path.to_str().context("non-utf8 leaf key path")?;
    let leaf_cert_str = leaf_cert_path.to_str().context("non-utf8 leaf cert path")?;
    let csr_str = csr_path.to_str().context("non-utf8 csr path")?;
    let ext_str = ext_path.to_str().context("non-utf8 ext path")?;
    let ca_cert_str = ca_cert_path.to_str().context("non-utf8 ca cert path")?;
    let ca_key_str = ca_key_path.to_str().context("non-utf8 ca key path")?;

    let mut san_entries = vec!["DNS:localhost".to_string(), "IP:127.0.0.1".to_string()];
    for host in san_hosts {
        let trimmed = host.trim();
        if trimmed.is_empty() || trimmed == "0.0.0.0" || trimmed == "127.0.0.1" || trimmed == "localhost" {
            continue;
        }
        if trimmed.parse::<std::net::IpAddr>().is_ok() {
            san_entries.push(format!("IP:{trimmed}"));
        } else {
            san_entries.push(format!("DNS:{trimmed}"));
        }
    }
    san_entries.sort();
    san_entries.dedup();

    let ext_content = format!(
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName={}\n",
        san_entries.join(",")
    );
    fs::write(&ext_path, ext_content)?;

    run_openssl(&["ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", leaf_key_str])?;
    set_owner_only_perms(leaf_key_path)?;
    run_openssl(&[
        "req",
        "-new",
        "-key",
        leaf_key_str,
        "-out",
        csr_str,
        "-subj",
        "/CN=danube-node",
    ])?;
    run_openssl(&[
        "x509",
        "-req",
        "-in",
        csr_str,
        "-CA",
        ca_cert_str,
        "-CAkey",
        ca_key_str,
        "-CAcreateserial",
        "-out",
        leaf_cert_str,
        "-days",
        "365",
        "-extfile",
        ext_str,
    ])?;

    let _ = fs::remove_file(&csr_path);
    let _ = fs::remove_file(&ext_path);
    let _ = fs::remove_file(&srl_path);
    Ok(())
}

fn load_or_create_jwt_secret(data_dir: &Path) -> Result<String> {
    let jwt_secret_path = data_dir.join("tls").join("jwt.secret");
    if jwt_secret_path.exists() {
        let existing = fs::read_to_string(&jwt_secret_path)?;
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    let mut raw = [0u8; 32];
    SystemRandom::new()
        .fill(&mut raw)
        .map_err(|_| anyhow::anyhow!("failed to generate JWT secret"))?;
    let secret = B64.encode(raw);
    fs::write(&jwt_secret_path, format!("{secret}\n"))?;
    set_owner_only_perms(&jwt_secret_path)?;
    Ok(secret)
}

fn ensure_bootstrap_admin_token(data_dir: &Path, jwt_secret: &str, issuer: &str) -> Result<PathBuf> {
    let token_path = data_dir.join("bootstrap_admin.token");
    if token_path.exists() {
        let existing = fs::read_to_string(&token_path)?;
        if !existing.trim().is_empty() {
            return Ok(token_path);
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let claims = Claims {
        iss: issuer.to_string(),
        exp: now + 365 * 24 * 3600,
        sub: BOOTSTRAP_ADMIN_SUBJECT.to_string(),
        principal_type: "user".to_string(),
        principal_name: BOOTSTRAP_ADMIN_SUBJECT.to_string(),
    };
    let token = create_token(&claims, jwt_secret)
        .map_err(|e| anyhow::anyhow!("failed to create bootstrap admin token: {e}"))?;
    fs::write(&token_path, format!("{token}\n"))?;
    set_owner_only_perms(&token_path)?;
    Ok(token_path)
}

/// Provisions node-local auto-TLS materials into `{data_dir}/tls/` before Raft startup
/// and ensures `{data_dir}/bootstrap_admin.token` exists with `0600` permissions.
pub(crate) fn provision_local_auto_tls(
    data_dir: &Path,
    san_hosts: &[String],
) -> Result<AutoTlsProvisioned> {
    let tls_dir = data_dir.join("tls");
    fs::create_dir_all(&tls_dir)
        .with_context(|| format!("failed to create {}", tls_dir.display()))?;

    let ca_cert_path = tls_dir.join("ca.pem");
    let ca_key_path = tls_dir.join("ca-key.pem");
    let cert_path = tls_dir.join("cert.pem");
    let key_path = tls_dir.join("key.pem");

    if !ca_cert_path.exists() || !ca_key_path.exists() {
        generate_ecdsa_ca(&ca_cert_path, &ca_key_path)?;
    }
    if !cert_path.exists() || !key_path.exists() {
        sign_leaf_cert(&ca_cert_path, &ca_key_path, &cert_path, &key_path, san_hosts)?;
    }

    let jwt_secret = load_or_create_jwt_secret(data_dir)?;
    let issuer = "danube-cluster".to_string();
    let bootstrap_token_path = ensure_bootstrap_admin_token(data_dir, &jwt_secret, &issuer)?;

    let ca_cert_pem = fs::read_to_string(&ca_cert_path)?;
    let ca_key_pem = fs::read_to_string(&ca_key_path).ok();

    Ok(AutoTlsProvisioned {
        tls: TlsConfig {
            ca_file: ca_cert_path.to_string_lossy().to_string(),
            cert_file: cert_path.to_string_lossy().to_string(),
            key_file: key_path.to_string_lossy().to_string(),
        },
        jwt: JwtConfig {
            secret_key: jwt_secret,
            issuer,
            expiration_time: 86_400,
        },
        ca_cert_pem,
        ca_key_pem,
        bootstrap_token_path,
    })
}

/// Persist or fetch the encrypted Cluster CA in Raft metadata (`/cluster/security/ca`).
/// When another node already initialized `/cluster/security/ca` and `DANUBE_CLUSTER_SECRET`
/// (or matching `cluster_ca.secret`) is shared, this syncs the cluster CA and re-signs the node cert.
pub(crate) async fn sync_cluster_ca_with_raft(
    store: &dyn MetadataStore,
    data_dir: &Path,
    provisioned: &AutoTlsProvisioned,
    san_hosts: &[String],
) -> Result<Option<String>> {
    let secret = load_or_create_cluster_secret(data_dir)?;
    if let Some(existing_val) = store.get(CLUSTER_CA_PATH, MetaOptions::None).await? {
        let record: ClusterCaRecord =
            serde_json::from_value(existing_val).context("invalid cluster CA record in Raft")?;
        if record.ca_cert_pem != provisioned.ca_cert_pem {
            let ca_key_bytes = decrypt_ca_key(
                &record.nonce_b64,
                &record.encrypted_ca_key_b64,
                &secret,
            )?;
            let tls_dir = data_dir.join("tls");
            let ca_cert_path = tls_dir.join("ca.pem");
            let ca_key_path = tls_dir.join("ca-key.pem");
            fs::write(&ca_cert_path, &record.ca_cert_pem)?;
            fs::write(&ca_key_path, &ca_key_bytes)?;
            set_owner_only_perms(&ca_key_path)?;
            sign_leaf_cert(
                &ca_cert_path,
                &ca_key_path,
                Path::new(&provisioned.tls.cert_file),
                Path::new(&provisioned.tls.key_file),
                san_hosts,
            )?;
        }
        return Ok(Some(record.jwt_secret_key));
    }

    if let Some(ref ca_key_pem) = provisioned.ca_key_pem {
        let (nonce_b64, encrypted_ca_key_b64) = encrypt_ca_key(ca_key_pem.as_bytes(), &secret)?;
        let created_at_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let record = ClusterCaRecord {
            ca_cert_pem: provisioned.ca_cert_pem.clone(),
            encrypted_ca_key_b64,
            nonce_b64,
            jwt_secret_key: provisioned.jwt.secret_key.clone(),
            created_at_secs,
        };
        if let Err(err) = store
            .put(CLUSTER_CA_PATH, serde_json::to_value(record)?, MetaOptions::None)
            .await
        {
            tracing::debug!(error = %err, "deferred /cluster/security/ca write (not leader)");
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use danube_core::metadata::MemoryStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    struct TestTempDir(PathBuf);

    impl TestTempDir {
        fn new(prefix: &str) -> Self {
            let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("danube_{prefix}_{}_{n}", std::process::id()));
            fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_encrypt_decrypt_ca_key_roundtrip() {
        let dir = TestTempDir::new("ca_rt");
        let secret = load_or_create_cluster_secret(dir.path()).unwrap();
        let sample_key = b"-----BEGIN EC PRIVATE KEY-----\nTESTKEYDATA\n-----END EC PRIVATE KEY-----\n";
        let (nonce, ciphertext) = encrypt_ca_key(sample_key, &secret).unwrap();
        let decrypted = decrypt_ca_key(&nonce, &ciphertext, &secret).unwrap();
        assert_eq!(decrypted, sample_key);

        let mut wrong_secret = secret;
        wrong_secret[0] ^= 0xff;
        assert!(decrypt_ca_key(&nonce, &ciphertext, &wrong_secret).is_err());
    }

    #[test]
    fn test_provision_local_auto_tls_creates_certs_and_bootstrap_token() {
        let dir = TestTempDir::new("prov");
        let hosts = vec!["127.0.0.1".to_string(), "broker1.local".to_string()];
        let provisioned = provision_local_auto_tls(dir.path(), &hosts).unwrap();

        assert!(Path::new(&provisioned.tls.ca_file).exists());
        assert!(Path::new(&provisioned.tls.cert_file).exists());
        assert!(Path::new(&provisioned.tls.key_file).exists());
        assert!(provisioned.bootstrap_token_path.exists());

        let token = fs::read_to_string(&provisioned.bootstrap_token_path).unwrap();
        let claims = danube_core::jwt::validate_token(token.trim(), &provisioned.jwt.secret_key)
            .expect("bootstrap token should validate against generated JWT secret");
        assert_eq!(claims.principal_name, BOOTSTRAP_ADMIN_SUBJECT);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let key_mode = fs::metadata(&provisioned.tls.key_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            let tok_mode = fs::metadata(&provisioned.bootstrap_token_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(key_mode, 0o600);
            assert_eq!(tok_mode, 0o600);
        }
    }

    #[tokio::test]
    async fn test_sync_cluster_ca_with_raft_stores_encrypted_record() {
        let dir = TestTempDir::new("sync");
        let store = MemoryStore::new().await.unwrap();
        let hosts = vec!["localhost".to_string()];
        let provisioned = provision_local_auto_tls(dir.path(), &hosts).unwrap();

        sync_cluster_ca_with_raft(&store, dir.path(), &provisioned, &hosts)
            .await
            .unwrap();

        let stored = store
            .get(CLUSTER_CA_PATH, MetaOptions::None)
            .await
            .unwrap()
            .expect("encrypted Cluster CA record stored");
        let rec: ClusterCaRecord = serde_json::from_value(stored).unwrap();
        assert_eq!(rec.ca_cert_pem, provisioned.ca_cert_pem);
        assert!(!rec.encrypted_ca_key_b64.contains("PRIVATE KEY"));
    }
}

