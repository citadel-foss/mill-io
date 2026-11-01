use mill_io::{error::Result, EventLoop, TaskPriority};

use mill_net::tcp::{
    traits::{ConnectionId, NetworkHandler},
    ServerContext, TcpServer, TcpServerConfig,
};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Instant,
};

static EVENT_LOOP: OnceLock<Arc<EventLoop>> = OnceLock::new();
fn event_loop() -> &'static Arc<EventLoop> {
    EVENT_LOOP.get_or_init(|| Arc::new(EventLoop::default()))
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpResponse {
    fn ok(body: &str, content_type: &str) -> Self {
        Self {
            status: 200,
            headers: vec![
                ("Content-Type".to_string(), content_type.to_string()),
                ("Content-Length".to_string(), body.len().to_string()),
            ],
            body: body.to_string(),
        }
    }

    fn html(body: &str) -> Self {
        Self::ok(body, "text/html")
    }

    fn json(body: &str) -> Self {
        Self::ok(body, "application/json")
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
        let status_text = match self.status {
            200 => "OK",
            404 => "Not Found",
            _ => "Unknown",
        };
        let mut response = format!("HTTP/1.1 {} {}\r\n", self.status, status_text);

        for (key, value) in &self.headers {
            response.push_str(&format!("{}: {}\r\n", key, value));
        }

        response.push_str("\r\n");
        response.push_str(&self.body);
        response.into_bytes()
    }
}

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

/// Simulates a cryptographic hashing operation (CPU-intensive)
fn simulate_crypto_hash(data: &str) -> String {
    let start = Instant::now();

    let mut hash: u64 = 0;
    for _ in 0..5_000_000 {
        for byte in data.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
    }

    let duration = start.elapsed();
    println!(
        "[COMPUTE] Crypto hash completed in {:?}, result: {:016x}",
        duration, hash
    );

    format!("{:016x}", hash)
}

/// Simulates image processing (CPU-intensive)
fn simulate_image_processing(width: u32, height: u32) -> String {
    let start = Instant::now();

    let mut checksum: u64 = 0;
    for y in 0..height {
        for x in 0..width {
            let r = ((x * 255) / width) as u8;
            let g = ((y * 255) / height) as u8;
            let b = (((x + y) * 127) / (width + height)) as u8;

            let filtered = (r as u64 * 299 + g as u64 * 587 + b as u64 * 114) / 1000;
            checksum = checksum.wrapping_add(filtered);
        }
    }

    let duration = start.elapsed();
    println!(
        "[COMPUTE] Image processing {}x{} completed in {:?}",
        width, height, duration
    );

    format!(
        "Processed {}x{} image, checksum: {}, time: {:?}",
        width, height, checksum, duration
    )
}

/// Simulates analytics computation (CPU-intensive, lower priority)
fn simulate_analytics(data_points: usize) -> String {
    let start = Instant::now();

    let mut sum: f64 = 0.0;
    let mut sum_sq: f64 = 0.0;

    for i in 0..data_points {
        let value = (i as f64 * 0.1).sin() * 100.0;
        sum += value;
        sum_sq += value * value;
    }

    let mean = sum / data_points as f64;
    let variance = (sum_sq / data_points as f64) - (mean * mean);
    let std_dev = variance.sqrt();

    let duration = start.elapsed();
    println!(
        "[COMPUTE] Analytics on {} points completed in {:?}",
        data_points, duration
    );

    format!(
        "Analyzed {} points: mean={:.2}, std_dev={:.2}, time: {:?}",
        data_points, mean, std_dev, duration
    )
}

struct ComputeHttpHandler;

impl ComputeHttpHandler {
    fn handle_request(&self, request: &HttpRequest) -> HttpResponse {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/") => HttpResponse::html(
                r#"<html>
<head><title>Mill-IO Compute Pool Demo</title></head>
<body>
    <h1>Mill-IO Compute Pool Demo</h1>
    <p>This server demonstrates I/O + compute separation.</p>
    <h2>Available Endpoints:</h2>
    <ul>
        <li><a href="/">/</a> - This page (quick I/O response)</li>
        <li><a href="/hash">/hash</a> - Crypto hashing (HIGH priority compute)</li>
        <li><a href="/process-image">/process-image</a> - Image processing (NORMAL priority)</li>
        <li><a href="/analytics">/analytics</a> - Analytics (LOW priority)</li>
        <li><a href="/burst">/burst</a> - Submit 9 tasks (mixed priorities)</li>
        <li><a href="/metrics">/metrics</a> - Compute pool metrics (JSON)</li>
    </ul>
</body>
</html>"#,
            ),

            ("GET", "/hash") => {
                println!("[INFO] Spawning HIGH priority crypto hash task");
                event_loop().spawn_compute_with_priority(
                    || {
                        let result = simulate_crypto_hash("secret_data_to_hash");
                        println!("[COMPUTE] Hash result ready: {}", result);
                    },
                    TaskPriority::High,
                );

                HttpResponse::html(
                    r#"<html>
<body>
    <h1>Crypto Hash Task Submitted</h1>
    <p>A HIGH priority crypto hashing task has been submitted to the compute pool.</p>
    <p>Check server console for results.</p>
    <p><a href="/metrics">View metrics</a> | <a href="/">Back</a></p>
</body>
</html>"#,
                )
            }

            ("GET", "/process-image") => {
                println!("[INFO] Spawning NORMAL priority image processing task");
                event_loop().spawn_compute(|| {
                    let result = simulate_image_processing(1000, 1000);
                    println!("[COMPUTE] Image processing result: {}", result);
                });

                HttpResponse::html(
                    r#"<html>
<body>
    <h1>Image Processing Task Submitted</h1>
    <p>A NORMAL priority image processing task (1000x1000) has been submitted.</p>
    <p>Check server console for results.</p>
    <p><a href="/metrics">View metrics</a> | <a href="/">Back</a></p>
</body>
</html>"#,
                )
            }

            ("GET", "/analytics") => {
                println!("[INFO] Spawning LOW priority analytics task");
                event_loop().spawn_compute_with_priority(
                    || {
                        let result = simulate_analytics(10_000_000);
                        println!("[COMPUTE] Analytics result: {}", result);
                    },
                    TaskPriority::Low,
                );

                HttpResponse::html(
                    r#"<html>
<body>
    <h1>Analytics Task Submitted</h1>
    <p>A LOW priority analytics task (10M data points) has been submitted.</p>
    <p>Check server console for results.</p>
    <p><a href="/metrics">View metrics</a> | <a href="/">Back</a></p>
</body>
</html>"#,
                )
            }

            ("GET", "/metrics") => {
                let metrics = event_loop().get_compute_metrics();
                let json = format!(
                    r#"{{
  "tasks_submitted": {},
  "tasks_completed": {},
  "tasks_failed": {},
  "active_workers": {},
  "queue_depth": {{
    "low": {},
    "normal": {},
    "high": {},
    "critical": {}
  }},
  "total_execution_time_ms": {}
}}"#,
                    metrics.tasks_submitted(),
                    metrics.tasks_completed(),
                    metrics.tasks_failed(),
                    metrics.active_workers(),
                    metrics.queue_depth_low(),
                    metrics.queue_depth_normal(),
                    metrics.queue_depth_high(),
                    metrics.queue_depth_critical(),
                    metrics.total_execution_time_ns() / 1_000_000
                );

                HttpResponse::json(&json)
            }

            ("GET", "/burst") => {
                println!("[INFO] Spawning burst of tasks with mixed priorities");

                for i in 0..3 {
                    let task_id = i;
                    event_loop().spawn_compute_with_priority(
                        move || {
                            println!("[COMPUTE] LOW priority task {} starting", task_id);
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            println!("[COMPUTE] LOW priority task {} done", task_id);
                        },
                        TaskPriority::Low,
                    );
                }

                for i in 0..3 {
                    let task_id = i;
                    event_loop().spawn_compute_with_priority(
                        move || {
                            println!("[COMPUTE] HIGH priority task {} starting", task_id);
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            println!("[COMPUTE] HIGH priority task {} done", task_id);
                        },
                        TaskPriority::High,
                    );
                }

                for i in 0..3 {
                    let task_id = i;
                    event_loop().spawn_compute_with_priority(
                        move || {
                            println!("[COMPUTE] CRITICAL priority task {} starting", task_id);
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            println!("[COMPUTE] CRITICAL priority task {} done", task_id);
                        },
                        TaskPriority::Critical,
                    );
                }

                HttpResponse::html(
                    r#"<html>
<body>
    <h1>Burst of Tasks Submitted</h1>
    <p>9 tasks submitted: 3 LOW, 3 HIGH, 3 CRITICAL priority.</p>
    <p>Watch the console to see priority ordering in action!</p>
    <p><a href="/metrics">View metrics</a> | <a href="/">Back</a></p>
</body>
</html>"#,
                )
            }

            _ => HttpResponse::not_found(),
        }
    }
}

impl NetworkHandler for ComputeHttpHandler {
    fn on_connect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        println!("[INFO] Client connected: {:?}", conn_id);
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        if let Some(request) = HttpRequest::parse(data) {
            println!(
                "[INFO] {} {} from {:?}",
                request.method, request.path, conn_id
            );
            let response = self.handle_request(&request);
            ctx.send_to(conn_id, &response.to_bytes())?;
        } else {
            let response = HttpResponse::not_found();
            ctx.send_to(conn_id, &response.to_bytes())?;
        }
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn_id: ConnectionId) -> Result<()> {
        println!("[INFO] Client disconnected: {:?}", conn_id);
        Ok(())
    }
}

fn main() -> Result<()> {
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:8080".parse().unwrap())
        .buffer_size(8192)
        .build();

    let server = Arc::new(TcpServer::new(config, ComputeHttpHandler)?);
    let addr = server.local_addr()?;
    let url = format!("http://{}", addr);

    println!("Server: {}", url);
    println!("Endpoints:");
    println!("  {url}/              - Home page");
    println!("  {url}/hash          - Crypto hash (HIGH priority)");
    println!("  {url}/process-image - Image processing (NORMAL)");
    println!("  {url}/analytics     - Analytics (LOW priority)");
    println!("  {url}/burst         - Submit 9 mixed tasks");
    println!("  {url}/metrics       - Pool metrics (JSON)");
    println!();

    server.start(event_loop(), event_loop().alloc_token())?;

    println!("Server started. Press Ctrl+C to stop.");

    event_loop().run()?;

    Ok(())
}
