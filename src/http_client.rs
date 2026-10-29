//! Small HTTP/1.1 client for the harness driver; no retries after uncertain mutations.
use crate::types::KvError;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

pub struct Client {
    address: SocketAddr,
    connection: Option<BufReader<TcpStream>>,
}
impl Client {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            connection: None,
        }
    }
    pub async fn request(
        &mut self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<(u16, Vec<u8>), KvError> {
        let result = self.exchange(method, path, body).await;
        if result.is_err() {
            self.connection = None;
        }
        result
    }
    async fn exchange(
        &mut self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<(u16, Vec<u8>), KvError> {
        if self.connection.is_none() {
            let stream = TcpStream::connect(self.address).await?;
            stream.set_nodelay(true)?;
            self.connection = Some(BufReader::new(stream));
        }
        let reader = self.connection.as_mut().unwrap();
        let header = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            self.address,
            body.len()
        );
        reader.get_mut().write_all(header.as_bytes()).await?;
        reader.get_mut().write_all(body).await?;
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let status = line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| KvError::protocol("missing HTTP status"))?
            .parse::<u16>()
            .map_err(|_| KvError::protocol("invalid HTTP status"))?;
        let mut length = None;
        let mut chunked = false;
        let mut close = false;
        let mut header_bytes = line.len();
        loop {
            line.clear();
            if reader.read_line(&mut line).await? == 0 {
                return Err(KvError::protocol("truncated HTTP headers"));
            }
            header_bytes += line.len();
            if header_bytes > 65536 {
                return Err(KvError::protocol("oversized HTTP headers"));
            }
            if line == "\r\n" {
                break;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| KvError::protocol("invalid HTTP header"))?;
            match name.to_ascii_lowercase().as_str() {
                "content-length" => {
                    length = Some(
                        value
                            .trim()
                            .parse::<usize>()
                            .map_err(|_| KvError::protocol("invalid HTTP length"))?,
                    )
                }
                "transfer-encoding" => {
                    if !value.trim().eq_ignore_ascii_case("chunked") {
                        return Err(KvError::protocol("unsupported transfer encoding"));
                    }
                    chunked = true;
                }
                "connection" => close = value.trim().eq_ignore_ascii_case("close"),
                _ => (),
            }
        }
        let mut data = Vec::new();
        if chunked {
            loop {
                line.clear();
                reader.read_line(&mut line).await?;
                let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
                    .map_err(|_| KvError::protocol("invalid HTTP chunk"))?;
                if size == 0 {
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).await? == 0 {
                            return Err(KvError::protocol("truncated chunk trailer"));
                        }
                        if line == "\r\n" {
                            break;
                        }
                    }
                    break;
                }
                if data.len() + size > 4 * 1024 * 1024 {
                    return Err(KvError::protocol("oversized HTTP body"));
                }
                let old = data.len();
                data.resize(old + size, 0);
                reader.read_exact(&mut data[old..]).await?;
                let mut ending = [0; 2];
                reader.read_exact(&mut ending).await?;
                if ending != *b"\r\n" {
                    return Err(KvError::protocol("invalid chunk ending"));
                }
            }
        } else {
            let size = if status == 204 {
                0
            } else {
                length.ok_or_else(|| KvError::protocol("HTTP response lacks framing"))?
            };
            if size > 4 * 1024 * 1024 {
                return Err(KvError::protocol("oversized HTTP body"));
            }
            data.resize(size, 0);
            reader.read_exact(&mut data).await?;
        }
        if close {
            self.connection = None;
        }
        Ok((status, data))
    }
}
