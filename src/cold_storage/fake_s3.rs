/// A minimal S3-compatible server, in process, for tests.
///
/// It is deliberately not a mock of `ColdStorage`: the point is to drive the **real** `my-s3`
/// client over a **real** socket, so the SigV4 signing, the `Range` header, the 206/204/404 status
/// codes and the way a key containing `/` is put on the wire are all exercised for real. Those are
/// exactly the places that can not be checked by reading the code.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Default)]
pub struct FakeS3State {
    /// Whatever path the client actually asked for, exactly as it arrived.
    pub objects: HashMap<String, Vec<u8>>,
    pub requests: Vec<String>,
}

pub struct FakeS3 {
    pub endpoint: String,
    pub state: Arc<Mutex<FakeS3State>>,
}

impl FakeS3 {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let state = Arc::new(Mutex::new(FakeS3State::default()));

        let server_state = state.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };

                let state = server_state.clone();

                tokio::spawn(async move {
                    let _ = handle(&mut socket, state).await;
                });
            }
        });

        Self {
            endpoint: format!("http://127.0.0.1:{}", port),
            state,
        }
    }

    pub fn object_paths(&self) -> Vec<String> {
        let mut result: Vec<String> = self.state.lock().unwrap().objects.keys().cloned().collect();
        result.sort();
        result
    }

    /// Every request line the client actually sent, so a test can assert on the wire traffic -
    /// how many round trips a cold read costs, for instance.
    pub fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }
}

async fn handle(
    socket: &mut tokio::net::TcpStream,
    state: Arc<Mutex<FakeS3State>>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];

    // Headers first
    let header_end = loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);

        if let Some(pos) = find_header_end(&buffer) {
            break pos;
        }
    };

    let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let mut lines = head.lines();

    let request_line = lines.next().unwrap_or_default().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut range: Option<(usize, Option<usize>)> = None;

    for line in lines {
        let lower = line.to_ascii_lowercase();

        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }

        if let Some(value) = lower.strip_prefix("range:") {
            range = parse_range(value.trim());
        }
    }

    // Then the body
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);

    state
        .lock()
        .unwrap()
        .requests
        .push(format!("{} {}", method, path));

    let response = match method.as_str() {
        "PUT" => {
            state.lock().unwrap().objects.insert(path.clone(), body);
            ok_response(200, "OK", Vec::new(), None)
        }
        "DELETE" => {
            state.lock().unwrap().objects.remove(&path);
            // What S3 actually answers a successful delete with
            ok_response(204, "No Content", Vec::new(), None)
        }
        "GET" => {
            let object = state.lock().unwrap().objects.get(&path).cloned();

            match object {
                None => not_found(),
                Some(content) => match range {
                    None => ok_response(200, "OK", content, None),
                    Some((from, to)) => {
                        if from >= content.len() {
                            range_not_satisfiable()
                        } else {
                            let to = to.unwrap_or(content.len() - 1).min(content.len() - 1);
                            let slice = content[from..=to].to_vec();
                            let content_range = format!("bytes {}-{}/{}", from, to, content.len());
                            ok_response(206, "Partial Content", slice, Some(content_range))
                        }
                    }
                },
            }
        }
        _ => ok_response(405, "Method Not Allowed", Vec::new(), None),
    };

    socket.write_all(response.as_slice()).await?;
    socket.flush().await?;

    Ok(())
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_range(value: &str) -> Option<(usize, Option<usize>)> {
    let value = value.strip_prefix("bytes=")?;
    let mut parts = value.splitn(2, '-');
    let from: usize = parts.next()?.trim().parse().ok()?;
    let to = parts
        .next()
        .and_then(|itm| itm.trim().parse::<usize>().ok());
    Some((from, to))
}

fn ok_response(code: u16, reason: &str, body: Vec<u8>, content_range: Option<String>) -> Vec<u8> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        code,
        reason,
        body.len()
    );

    if let Some(content_range) = content_range {
        head.push_str(&format!("Content-Range: {}\r\n", content_range));
    }

    head.push_str("\r\n");

    let mut result = head.into_bytes();
    result.extend_from_slice(body.as_slice());
    result
}

fn not_found() -> Vec<u8> {
    let body =
        b"<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"
            .to_vec();
    ok_response(404, "Not Found", body, None)
}

fn range_not_satisfiable() -> Vec<u8> {
    ok_response(416, "Requested Range Not Satisfiable", Vec::new(), None)
}
