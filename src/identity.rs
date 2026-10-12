//! A registered runner: what GitHub knows it as, and the private key it
//! authenticates with. One file per registration under `<data_dir>/runners`.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rand::rngs::OsRng;
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use rsa::signature::{RandomizedSigner, SignatureEncoding, Signer};
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// What a runner is registered to.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Scope {
    Repo { owner: String, repo: String },
    Org { org: String },
}

impl Scope {
    pub fn github_url(&self, server: &str) -> String {
        let server = server.trim_end_matches('/');
        match self {
            Scope::Repo { owner, repo } => format!("{server}/{owner}/{repo}"),
            Scope::Org { org } => format!("{server}/{org}"),
        }
    }

    /// REST path prefix for runner management endpoints.
    pub fn api_prefix(&self) -> String {
        match self {
            Scope::Repo { owner, repo } => format!("repos/{owner}/{repo}"),
            Scope::Org { org } => format!("orgs/{org}"),
        }
    }

    fn dir_parts(&self) -> (String, String) {
        match self {
            Scope::Repo { owner, repo } => (owner.clone(), repo.clone()),
            // '@' cannot appear in a repository name, so this cannot collide.
            Scope::Org { org } => (org.clone(), "@org".to_string()),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::Repo { owner, repo } => write!(f, "{owner}/{repo}"),
            Scope::Org { org } => write!(f, "{org}"),
        }
    }
}

impl FromStr for Scope {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.trim().trim_matches('/').split('/').collect();
        match parts.as_slice() {
            [org] if !org.is_empty() => Ok(Scope::Org {
                org: org.to_string(),
            }),
            [owner, repo] if !owner.is_empty() && !repo.is_empty() => Ok(Scope::Repo {
                owner: owner.to_string(),
                repo: repo.to_string(),
            }),
            _ => bail!("'{s}' is neither 'owner/repo' nor 'org'"),
        }
    }
}

impl TryFrom<String> for Scope {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl From<Scope> for String {
    fn from(s: Scope) -> String {
        s.to_string()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    pub scope: Scope,
    pub github_url: String,
    pub name: String,
    pub agent_id: u64,
    pub pool_id: i64,
    #[serde(default)]
    pub pool_name: String,
    /// Legacy pipelines tenant URL (empty for runner-admin registrations).
    #[serde(default)]
    pub server_url: String,
    /// Broker URL, when the registration uses the v2 message flow.
    #[serde(default)]
    pub server_url_v2: Option<String>,
    #[serde(default)]
    pub use_v2_flow: bool,
    pub client_id: String,
    pub authorization_url: String,
    #[serde(default)]
    pub authorization_url_v2: Option<String>,
    /// Sign client assertions with PS256 (true) or RS256 (false).
    #[serde(default = "default_true")]
    pub require_fips: bool,
    pub labels: Vec<String>,
    /// PKCS#8 PEM. This file is the runner's credential; keep it private.
    pub private_key_pem: String,
    pub registered_version: String,
    #[serde(default)]
    pub last_session_id: Option<String>,
}

fn default_true() -> bool {
    true
}

impl Identity {
    pub fn key(&self) -> Result<RsaPrivateKey> {
        RsaPrivateKey::from_pkcs8_pem(&self.private_key_pem)
            .context("runner private key is unreadable")
    }

    pub fn path(&self, data_dir: &Path) -> PathBuf {
        identity_path(data_dir, &self.scope, &self.name)
    }

    pub fn save(&self, data_dir: &Path) -> Result<()> {
        let json = serde_json::to_vec_pretty(self)?;
        crate::util::write_private(&self.path(data_dir), &json)
    }

    pub fn remove_file(&self, data_dir: &Path) -> Result<()> {
        let path = self.path(data_dir);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        if let Some(dir) = path.parent() {
            let _ = std::fs::remove_dir(dir); // only succeeds if empty
            if let Some(owner_dir) = dir.parent() {
                let _ = std::fs::remove_dir(owner_dir);
            }
        }
        Ok(())
    }

    /// Unique, human readable key for logs and maps.
    pub fn display_key(&self) -> String {
        format!("{}#{}", self.scope, self.name)
    }

    /// Signed JWT used as the OAuth `client_assertion`, matching
    /// `VssOAuthJwtBearerAssertion`: iss = sub = client id, aud = token URL,
    /// five minute lifetime.
    pub fn client_assertion(&self, key: &RsaPrivateKey, audience: &str) -> Result<String> {
        let alg = if self.require_fips { "PS256" } else { "RS256" };
        let header = serde_json::json!({ "typ": "JWT", "alg": alg });
        // Back-date by 30s to tolerate small clock skew against the token service.
        let now = crate::util::now_unix().saturating_sub(30);
        let payload = serde_json::json!({
            "sub": self.client_id,
            "jti": uuid::Uuid::new_v4().to_string(),
            "iss": self.client_id,
            "aud": audience,
            "nbf": now,
            "exp": now + 300,
        });
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload)?)
        );
        let signature = if self.require_fips {
            let signer = rsa::pss::BlindedSigningKey::<Sha256>::new(key.clone());
            signer
                .sign_with_rng(&mut OsRng, signing_input.as_bytes())
                .to_vec()
        } else {
            let signer = rsa::pkcs1v15::SigningKey::<Sha256>::new(key.clone());
            signer.sign(signing_input.as_bytes()).to_vec()
        };
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }
}

pub fn identity_path(data_dir: &Path, scope: &Scope, name: &str) -> PathBuf {
    let (a, b) = scope.dir_parts();
    data_dir
        .join("runners")
        .join(a)
        .join(b)
        .join(format!("{name}.json"))
}

pub fn load_all(data_dir: &Path) -> Result<Vec<Identity>> {
    let mut out = Vec::new();
    let root = data_dir.join("runners");
    if !root.exists() {
        return Ok(out);
    }
    for owner in std::fs::read_dir(&root)? {
        let owner = owner?.path();
        if !owner.is_dir() {
            continue;
        }
        for scope_dir in std::fs::read_dir(&owner)? {
            let scope_dir = scope_dir?.path();
            if !scope_dir.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(&scope_dir)? {
                let file = file?.path();
                if file.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let raw = std::fs::read(&file)?;
                match serde_json::from_slice::<Identity>(&raw) {
                    Ok(id) => out.push(id),
                    Err(e) => tracing::warn!("ignoring unreadable {}: {e}", file.display()),
                }
            }
        }
    }
    out.sort_by(|a, b| (&a.scope, &a.name).cmp(&(&b.scope, &b.name)));
    Ok(out)
}

pub fn generate_key() -> Result<RsaPrivateKey> {
    Ok(RsaPrivateKey::new(&mut OsRng, 2048)?)
}

pub fn key_to_pem(key: &RsaPrivateKey) -> Result<String> {
    Ok(key.to_pkcs8_pem(LineEnding::LF)?.to_string())
}

/// `(exponent, modulus)` as big-endian bytes, the shape of .NET's `RSAParameters`.
pub fn public_parts(key: &RsaPrivateKey) -> (Vec<u8>, Vec<u8>) {
    let public = RsaPublicKey::from(key);
    (public.e().to_bytes_be(), public.n().to_bytes_be())
}

/// The `<RSAKeyValue>` XML that the runner-admin registration API expects.
pub fn public_key_xml(key: &RsaPrivateKey) -> String {
    let (e, n) = public_parts(key);
    format!(
        "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
        STANDARD.encode(n),
        STANDARD.encode(e)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::signature::Verifier;

    fn test_identity(fips: bool, key: &RsaPrivateKey) -> Identity {
        Identity {
            scope: "octo/demo".parse().unwrap(),
            github_url: "https://github.com/octo/demo".into(),
            name: "hive".into(),
            agent_id: 7,
            pool_id: 1,
            pool_name: "Default".into(),
            server_url: String::new(),
            server_url_v2: None,
            use_v2_flow: false,
            client_id: "11111111-2222-3333-4444-555555555555".into(),
            authorization_url: "https://token.example/oauth".into(),
            authorization_url_v2: None,
            require_fips: fips,
            labels: vec![],
            private_key_pem: key_to_pem(key).unwrap(),
            registered_version: "2.338.0".into(),
            last_session_id: None,
        }
    }

    #[test]
    fn scope_round_trip() {
        let r: Scope = "owner/repo".parse().unwrap();
        assert_eq!(r.to_string(), "owner/repo");
        assert_eq!(r.api_prefix(), "repos/owner/repo");
        let o: Scope = "my-org".parse().unwrap();
        assert_eq!(o.api_prefix(), "orgs/my-org");
        assert!("a/b/c".parse::<Scope>().is_err());
    }

    #[test]
    fn client_assertion_verifies_with_public_key() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let public = RsaPublicKey::from(&key);
        for fips in [true, false] {
            let id = test_identity(fips, &key);
            let jwt = id
                .client_assertion(&key, "https://token.example/oauth")
                .unwrap();
            let parts: Vec<&str> = jwt.split('.').collect();
            assert_eq!(parts.len(), 3);
            let header: serde_json::Value =
                serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
            assert_eq!(header["alg"], if fips { "PS256" } else { "RS256" });
            let payload: serde_json::Value =
                serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
            assert_eq!(payload["iss"], id.client_id);
            assert_eq!(payload["sub"], id.client_id);
            assert_eq!(payload["aud"], "https://token.example/oauth");
            let sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
            let input = format!("{}.{}", parts[0], parts[1]);
            if fips {
                let vk = rsa::pss::VerifyingKey::<Sha256>::new(public.clone());
                let sig = rsa::pss::Signature::try_from(sig.as_slice()).unwrap();
                vk.verify(input.as_bytes(), &sig).unwrap();
            } else {
                let vk = rsa::pkcs1v15::VerifyingKey::<Sha256>::new(public.clone());
                let sig = rsa::pkcs1v15::Signature::try_from(sig.as_slice()).unwrap();
                vk.verify(input.as_bytes(), &sig).unwrap();
            }
        }
    }

    #[test]
    fn public_key_shape_matches_dotnet() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let (e, n) = public_parts(&key);
        assert_eq!(e, vec![1, 0, 1]);
        assert_eq!(n.len(), 256);
        assert!(public_key_xml(&key).contains("<Exponent>AQAB</Exponent>"));
    }
}
