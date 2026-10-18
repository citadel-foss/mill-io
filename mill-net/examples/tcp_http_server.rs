use mill_io::error::Result;
use mill_io::EventLoop;
use mill_net::errors::{NetworkError, NetworkEvent};
use mill_net::tcp::{config::TcpServerConfig, traits::*, ConnectionId, ServerContext, TcpServer};
use std::collections::HashMap;
use std::sync::Arc;

/// HTTP response builder
struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpResponse {
    fn ok(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![
                ("Content-Type".to_string(), "text/html".to_string()),
                ("Content-Length".to_string(), body.len().to_string()),
            ],
            body: body.to_string(),
        }
    }

    fn not_found() -> Self {
        let body = "<h1>404 Not Found</h1>";
        Self {
            status: 404,
            headers: vec![
                ("Content-Type".to_string(), "text/html".to_string()),
                ("Content-Length".to_string(), body.len().to_string()),
            ],
            body: body.to_string(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\n",
            self.status,
            match self.status {
                200 => "OK",
                404 => "Not Found",
                _ => "Unknown",
            }
        );

        for (key, value) in &self.headers {
            response.push_str(&format!("{}: {}\r\n", key, value));
        }

        response.push_str("\r\n");
        response.push_str(&self.body);

        response.into_bytes()
    }
}

/// Simple HTTP request parser
struct HttpRequest {
    method: String,
    path: String,
    #[allow(dead_code)]
    headers: HashMap<String, String>,
}

impl HttpRequest {
    fn parse(data: &[u8]) -> Option<Self> {
        let request_str = String::from_utf8_lossy(data);
        let lines: Vec<&str> = request_str.lines().collect();

        if lines.is_empty() {
            return None;
        }

        let request_line_parts: Vec<&str> = lines[0].split_whitespace().collect();
        if request_line_parts.len() < 2 {
            return None;
        }

        let method = request_line_parts[0].to_string();
        let path = request_line_parts[1].to_string();

        let mut headers = HashMap::new();
        for line in lines.iter().skip(1) {
            if line.is_empty() {
                break;
            }
            if let Some((key, value)) = line.split_once(':') {
                headers.insert(key.trim().to_lowercase(), value.trim().to_string());
            }
        }

        Some(HttpRequest {
            method,
            path,
            headers,
        })
    }
}

#[derive(Clone, Default)]
struct HttpHandler;

impl HttpHandler {
    fn handle_http_request(&self, data: &[u8]) -> HttpResponse {
        if let Some(request) = HttpRequest::parse(data) {
            match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/") => HttpResponse::ok(
                    "<h1>Welcome to Mill-IO HTTP Server!</h1><p>A simple HTTP server built with the mill-io event loop.</p>",
                ),
                ("GET", "/about") => HttpResponse::ok(
                    "<h1>About</h1><p>This is a demonstration HTTP server using mill-io event loop library.</p>",
                ),
                ("GET", "/health") => {
                    HttpResponse::ok("<h1>Health Check</h1><p>Server is running!</p>")
                }
                _ => HttpResponse::not_found(),
            }
        } else {
            HttpResponse::not_found()
        }
    }
}

impl NetworkHandler for HttpHandler {
    fn on_event(&self, _ctx: &ServerContext, event: NetworkEvent) -> Result<()> {
        match event {
            NetworkEvent::ConnectionEstablished(conn_id, addr) => {
                println!("[INFO] New connection: {:?} from {}", conn_id, addr);
            }
            NetworkEvent::ConnectionClosed(conn_id) => {
                println!("[INFO] Connection closed: {:?}", conn_id);
            }
            NetworkEvent::Listening(addr) => {
                println!("[INFO] Server listening on {}", addr);
            }
        }
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        let response = self.handle_http_request(data);
        let response_bytes = response.to_bytes();
        ctx.send_to(conn_id, &response_bytes)?;
        Ok(())
    }

    fn on_error(&self, _ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        if let Some(id) = conn_id {
            eprintln!("[ERROR] Error on connection {:?}: {}", id, error);
        } else {
            eprintln!("[ERROR] Server error: {}", error);
        }
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::default());

    let config = TcpServerConfig::builder()
        .address("127.0.0.1:8080".parse().unwrap())
        .build();

    let handler = HttpHandler;
    let server = Arc::new(TcpServer::new(config.clone(), handler)?);

    server.start(&event_loop, event_loop.alloc_token())?;

    let addr = format!("http://{}", config.address);
    println!("HTTP Server starting on {}", addr);
    println!("Available routes:");
    println!("  GET {}/       - Home page", addr);
    println!("  GET {}/about  - About page", addr);
    println!("  GET {}/health - Health check", addr);

    event_loop.run()?;

    Ok(())
}
