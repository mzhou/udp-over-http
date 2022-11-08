mod danger;

use std::convert::Infallible;
use std::io::Error as IoError;
use std::net::{AddrParseError, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use hyper::body::{Bytes, HttpBody, Sender as BodySender};
use hyper::client::{Client, HttpConnector};
use hyper::http::Method;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Request, Response, Server, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::{ClientConfig, ALL_CIPHER_SUITES, ALL_KX_GROUPS, ALL_VERSIONS};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::spawn;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{
    channel as broadcast_channel, Receiver as BroadcastReceiver, Sender as BroadcastSender,
};
use tokio::time::sleep;

use crate::danger::NoCertificateVerification;

#[derive(Clone)]
struct AppContext {
    broadcast_sender: BroadcastSender<Bytes>,
    shared: Arc<AppContextShared>,
}

struct AppContextShared {
    udp_socket: UdpSocket,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "")]
    http_listen: String,
    #[arg(long, default_value = "[::]:0")]
    udp_bind: String,
    #[arg(long)]
    udp_connect: String,
    #[arg(long, default_value = "")]
    url: String,
}

type HttpsClient = Client<HttpsConnector<HttpConnector>>;

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    AddrParse(#[from] AddrParseError),
    #[error(transparent)]
    Hyper(#[from] HyperError),
    #[error(transparent)]
    Io(#[from] IoError),
}

async fn forward(mut receiver: BroadcastReceiver<Bytes>, mut sender: BodySender) {
    loop {
        let recv_result = receiver.recv().await;
        match recv_result {
            Ok(data) => {
                let send_result = sender.send_data(data).await;
                if let Err(e) = send_result {
                    eprintln!("forward send {:?}", e);
                    break;
                }
            }
            Err(RecvError::Lagged(skipped)) => {
                eprintln!("forward recv skipped {}", skipped);
            }
            Err(RecvError::Closed) => {
                eprintln!("forward recv closed");
                break;
            }
        }
    }
}

async fn handle(
    context: AppContext,
    _addr: SocketAddr,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    match *req.method() {
        Method::POST => {
            eprintln!("handle POST");
            let mut body = req.into_body();
            let mut buf = [0u8; 2 + 64 * 1024];
            let mut buf_used = 0usize;
            let udp_socket = &context.shared.udp_socket;
            loop {
                let Some(Ok(chunk)) = body.data().await else {
                    eprintln!("handle POST no more chunks");
                    break;
                };
                if buf_used == 0 {
                    // fast path
                    if chunk.len() < 2 {
                        eprintln!("POST chunk shorter than 2 not implementeD");
                        break;
                    }
                    let size = (chunk[0] as usize) | ((chunk[1] as usize) << 8);
                    if size != chunk.len() - 2 {
                        eprintln!("POST chunk non-exact not implementeD");
                        break;
                    }
                    let packet = &chunk[2..];
                    let send_result = udp_socket.send(packet).await;
                    if let Err(e) = send_result {
                        eprintln!("handle send {:?}", e);
                    }
                } else {
                    eprintln!("POST chunk while buffer not empty not implemented");
                    break;
                }
            }
            Ok(Response::new(Body::empty()))
        }
        _ => {
            let (sender, body) = Body::channel();
            let _ = spawn(forward(context.broadcast_sender.subscribe(), sender));
            Ok(Response::new(body))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let udp_bind: SocketAddr = args.udp_bind.parse()?;
    let udp_connect: SocketAddr = args.udp_connect.parse()?;

    let udp_socket = UdpSocket::bind(udp_bind).await?;

    eprintln!("UDP bound to {:?}", udp_bind);

    udp_socket.connect(udp_connect).await?;
    eprintln!("UDP connected to {:?}", udp_connect);

    let (broadcast_sender, _broadcast_receiver) = broadcast_channel(1024);

    let context = AppContext {
        broadcast_sender,
        shared: Arc::new(AppContextShared { udp_socket }),
    };

    let mut tasks = FuturesUnordered::new();

    let udp_reader_task = spawn(udp_reader(context.clone()));
    tasks.push(udp_reader_task);

    if !args.url.is_empty() {
        let request_task = spawn(requester(context.clone(), args.url));
        tasks.push(request_task);
    }

    if !args.http_listen.is_empty() {
        let http_listen: SocketAddr = args.http_listen.parse()?;

        let make_service = make_service_fn(move |conn: &AddrStream| {
            let context = context.clone();
            let addr = conn.remote_addr();
            let service = service_fn(move |req| handle(context.clone(), addr, req));

            async move { Ok::<_, Infallible>(service) }
        });

        let server = Server::bind(&http_listen).serve(make_service);

        eprintln!("HTTP listening on {:?}", server.local_addr());

        let server_task = spawn(async {
            let _ = server.await;
        });
        tasks.push(server_task);
    }

    tasks.next().await;

    Ok(())
}

fn make_https_client() -> HttpsClient {
    let mut http_connector = HttpConnector::new();
    http_connector.enforce_http(false);
    http_connector.set_nodelay(true);
    let https = HttpsConnectorBuilder::new()
        .with_tls_config(
            ClientConfig::builder()
                .with_cipher_suites(&ALL_CIPHER_SUITES)
                .with_kx_groups(&ALL_KX_GROUPS)
                .with_protocol_versions(ALL_VERSIONS)
                .unwrap()
                .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {}))
                .with_no_client_auth(),
        )
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = Client::builder().build::<_, Body>(https);
    client
}

async fn requester(context: AppContext, url: String) {
    loop {
        request_once(context.clone(), url.clone()).await;
        sleep(Duration::from_millis(100)).await;
    }
}

async fn request_once(context: AppContext, url: String) {
    let udp_socket = &context.shared.udp_socket;
    let client = make_https_client();
    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let res = match client.request(req).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("request_once request {:?}", e);
            return;
        }
    };
    if res.status() != StatusCode::OK {
        eprintln!("request_once status {}", res.status());
        return;
    }
    let mut buf = BytesMut::with_capacity(1024 * 1024);
    let mut body = res.into_body();
    while let Some(Ok(chunk)) = body.data().await {
        buf.extend_from_slice(chunk.as_ref());
        // keep trying to pop packets off
        loop {
            if buf.len() < 2 {
                // need more data
                break;
            }
            let size = (buf[0] as usize) | ((buf[1] as usize) << 8);
            if buf.len() < 2 + size {
                // need more data
                break;
            }
            let packet_with_size = buf.split_to(2 + size);
            let packet = &packet_with_size[2..];
            let send_result = udp_socket.send(packet).await;
            if let Err(e) = send_result {
                eprintln!("request_once send {:?}", e);
            }
        }
    }
    eprintln!("request_once done");
}

async fn udp_reader(context: AppContext) {
    let broadcast_sender = context.broadcast_sender;
    let udp_socket = &context.shared.udp_socket;
    loop {
        let mut buf = BytesMut::zeroed(2 + 64 * 1024);
        let recv_result = udp_socket.recv(&mut buf[2..]).await;
        match recv_result {
            Ok(size) => {
                let size_u16: u16 = size.try_into().unwrap();
                buf[0] = (size_u16 & 0xff) as u8;
                buf[1] = (size_u16 >> 8) as u8;
                buf.truncate(2 + size);
                let _ = broadcast_sender.send(buf.into());
            }
            Err(e) => {
                eprintln!("recv err {:?}", e);
                break;
            }
        }
    }
}
