//! TLS client over rustls (ring provider), as a sans-IO mill-net layer.

use super::{TlsConfig, TlsVerify};
use crate::ChainError;
use mill_net::tcp::Layer;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::{ring, verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, ClientConnection, DigitallySignedStruct, OtherError,
    RootCertStore, SignatureScheme,
};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::Arc;

/// A client config and the name to verify, shared by every connection to one backend.
pub(crate) struct TlsSetup {
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
}

impl TlsSetup {
    /// Builds the verifier for `tls`, verifying `host` unless `tls.server_name` is set.
    /// Reads a CA file now, not on the loop thread.
    pub(crate) fn new(tls: &TlsConfig, host: &str) -> Result<Self, ChainError> {
        let name = tls.server_name.as_deref().unwrap_or(host);
        let bare = name
            .strip_prefix('[')
            .and_then(|n| n.strip_suffix(']'))
            .unwrap_or(name);
        let name = ServerName::try_from(bare.to_string()).map_err(|e| {
            ChainError::Transport(format!("invalid TLS server name {:?}: {}", name, e))
        })?;
        let provider = Arc::new(ring::default_provider());
        let verifier: Arc<dyn ServerCertVerifier> = match &tls.verify {
            TlsVerify::WebPki => {
                let roots = webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect();
                onion_aware(roots, &provider)?
            }
            TlsVerify::CaFile(path) => {
                let mut roots = RootCertStore::empty();
                let certs = CertificateDer::pem_file_iter(path).map_err(|e| {
                    ChainError::Transport(format!("cannot read CA file {}: {}", path.display(), e))
                })?;
                for cert in certs {
                    let cert = cert.map_err(|e| {
                        ChainError::Transport(format!("bad CA file {}: {}", path.display(), e))
                    })?;
                    roots.add(cert).map_err(|e| {
                        ChainError::Transport(format!("bad CA in {}: {}", path.display(), e))
                    })?;
                }
                if roots.is_empty() {
                    return Err(ChainError::Transport(format!(
                        "no certificates in CA file {}",
                        path.display()
                    )));
                }
                onion_aware(roots, &provider)?
            }
            TlsVerify::PinSha256(pin) => Arc::new(Pinned {
                pin: Some(*pin),
                provider: Arc::clone(&provider),
            }),
            #[cfg(feature = "testing")]
            TlsVerify::InsecureForTests => Arc::new(Pinned {
                pin: None,
                provider: Arc::clone(&provider),
            }),
        };
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| ChainError::Transport(format!("TLS setup: {}", e)))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        Ok(Self {
            config: Arc::new(config),
            name,
        })
    }

    pub(crate) fn layer(&self) -> io::Result<TlsLayer> {
        let conn = ClientConnection::new(Arc::clone(&self.config), self.name.clone())
            .map_err(tls_error)?;
        Ok(TlsLayer { conn })
    }
}

fn onion_aware(
    roots: RootCertStore,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, ChainError> {
    let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(provider))
        .build()
        .map_err(|e| ChainError::Transport(format!("TLS setup: {}", e)))?;
    Ok(Arc::new(OnionAware { inner }))
}

/// A WebPKI verifier that skips only the name check for `.onion` hosts, whose address
/// already authenticates the server.
#[derive(Debug)]
struct OnionAware {
    inner: Arc<WebPkiServerVerifier>,
}

fn is_onion(name: &ServerName<'_>) -> bool {
    match name {
        ServerName::DnsName(dns) => {
            let dns = dns.as_ref().trim_end_matches('.');
            dns.len() > ".onion".len()
                && dns[dns.len() - ".onion".len()..].eq_ignore_ascii_case(".onion")
        }
        _ => false,
    }
}

impl ServerCertVerifier for OnionAware {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let result = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        );
        match result {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) if is_onion(server_name) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Trusts exactly the certificate whose DER hashes to `pin`, whatever its name, issuer or
/// dates; with no pin, trusts any certificate. Handshake signatures are always checked
/// against the presented certificate.
#[derive(Debug)]
struct Pinned {
    pin: Option<[u8; 32]>,
    provider: Arc<CryptoProvider>,
}

#[derive(Debug)]
struct PinMismatch {
    got: [u8; 32],
}

impl fmt::Display for PinMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "certificate does not match the pinned SHA-256; it hashes to "
        )?;
        self.got.iter().try_for_each(|b| write!(f, "{:02x}", b))
    }
}

impl std::error::Error for PinMismatch {}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let Some(pin) = self.pin else {
            return Ok(ServerCertVerified::assertion());
        };
        let got: [u8; 32] = Sha256::digest(end_entity.as_ref()).into();
        if got == pin {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(CertificateError::Other(OtherError(Arc::new(PinMismatch { got }))).into())
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn tls_error(error: rustls::Error) -> io::Error {
    let text = match &error {
        rustls::Error::InvalidCertificate(CertificateError::Other(OtherError(inner))) => {
            inner.to_string()
        }
        other => other.to_string(),
    };
    io::Error::new(io::ErrorKind::InvalidData, format!("TLS: {}", text))
}

/// One TLS client session.
pub(crate) struct TlsLayer {
    conn: ClientConnection,
}

impl TlsLayer {
    fn flush(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        while self.conn.wants_write() {
            self.conn.write_tls(out)?;
        }
        Ok(())
    }
}

impl Layer for TlsLayer {
    fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        self.flush(out)
    }

    fn decode(&mut self, mut input: &[u8], up: &mut Vec<u8>, out: &mut Vec<u8>) -> io::Result<()> {
        // rustls buffers a bounded amount, so each read is processed before the next.
        while !input.is_empty() {
            self.conn.read_tls(&mut input)?;
            let state = self.conn.process_new_packets().map_err(tls_error)?;
            let len = state.plaintext_bytes_to_read();
            if len > 0 {
                let start = up.len();
                up.resize(start + len, 0);
                self.conn.reader().read_exact(&mut up[start..])?;
            }
        }
        self.flush(out)
    }

    fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        let mut rest = frame;
        while !rest.is_empty() {
            let written = self.conn.writer().write(rest)?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "TLS: session refused plaintext",
                ));
            }
            rest = &rest[written..];
            self.flush(out)?;
        }
        Ok(())
    }

    fn is_ready(&self) -> bool {
        !self.conn.is_handshaking()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> ServerName<'static> {
        ServerName::try_from(s.to_string()).unwrap()
    }

    #[test]
    fn onion_names_are_recognised() {
        assert!(is_onion(&name("abcdefghijklmnop.onion")));
        assert!(is_onion(&name("x.ONION")));
        assert!(!is_onion(&name("onion")));
        assert!(!is_onion(&name("example.com")));
        assert!(!is_onion(&name("notonion.com")));
        assert!(!is_onion(&name("10.0.0.1")));
    }

    #[test]
    fn pin_mismatch_names_the_hash_it_saw() {
        let error = tls_error(
            CertificateError::Other(OtherError(Arc::new(PinMismatch { got: [0xab; 32] }))).into(),
        );
        let text = error.to_string();
        assert!(text.starts_with("TLS: certificate does not match the pinned SHA-256"));
        assert!(text.ends_with(&"ab".repeat(32)));
    }

    #[test]
    fn bad_server_names_are_refused() {
        let tls = TlsConfig {
            server_name: Some("not a name".into()),
            verify: TlsVerify::WebPki,
        };
        assert!(TlsSetup::new(&tls, "host").is_err());
        let tls = TlsConfig {
            server_name: None,
            verify: TlsVerify::PinSha256([0; 32]),
        };
        assert!(TlsSetup::new(&tls, "[::1]").is_ok());
        assert!(TlsSetup::new(&tls, "example.onion").is_ok());
    }

    #[test]
    fn missing_or_empty_ca_files_are_refused() {
        let tls = TlsConfig {
            server_name: None,
            verify: TlsVerify::CaFile("/nonexistent/ca.pem".into()),
        };
        assert!(TlsSetup::new(&tls, "host").is_err());
        let empty =
            std::env::temp_dir().join(format!("mill-chain-empty-{}.pem", std::process::id()));
        std::fs::write(&empty, "").unwrap();
        let tls = TlsConfig {
            server_name: None,
            verify: TlsVerify::CaFile(empty.clone()),
        };
        let result = TlsSetup::new(&tls, "host");
        let _ = std::fs::remove_file(&empty);
        assert!(matches!(result, Err(ChainError::Transport(m)) if m.contains("no certificates")));
    }
}
