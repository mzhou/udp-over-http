use std::convert::Infallible;
use std::io::Error as IoError;
use std::net::{AddrParseError, SocketAddr};
use std::sync::Arc;

use clap::Parser;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Request, Response, Server};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::{select, spawn};

#[derive(Clone)]
struct AppContext {
    shared: Arc<AppContextShared>,
}

struct AppContextShared {
    udp_socket: UdpSocket,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "[::]:0")]
    http_listen: String,
    #[arg(long, default_value = "[::]:0")]
    udp_bind: String,
    #[arg(long)]
    udp_connect: String,
}

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    AddrParse(#[from] AddrParseError),
    #[error(transparent)]
    Hyper(#[from] HyperError),
    #[error(transparent)]
    Io(#[from] IoError),
}

async fn handle(
    context: AppContext,
    addr: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    Ok(Response::new(Body::from("Hello World")))
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let http_listen: SocketAddr = args.http_listen.parse()?;
    let udp_bind: SocketAddr = args.udp_bind.parse()?;
    let udp_connect: SocketAddr = args.udp_connect.parse()?;

    let udp_socket = UdpSocket::bind(udp_bind).await?;

    eprintln!("UDP bound to {:?}", udp_bind);

    udp_socket.connect(udp_connect).await?;
    eprintln!("UDP connected to {:?}", udp_connect);

    let context = AppContext {
        shared: Arc::new(AppContextShared { udp_socket }),
    };

    let udp_reader_task = spawn(udp_reader(context.clone()));

    let make_service = make_service_fn(move |conn: &AddrStream| {
        let context = context.clone();
        let addr = conn.remote_addr();
        let service = service_fn(move |req| handle(context.clone(), addr, req));

        async move { Ok::<_, Infallible>(service) }
    });

    let server = Server::bind(&http_listen).serve(make_service);

    eprintln!("HTTP listening on {:?}", server.local_addr());

    let server_task = spawn(server);

    select! {
        _ = server_task => {
        },
        _ = udp_reader_task => {
        },
    };

    Ok(())
}

async fn udp_reader(context: AppContext) {
    let mut buf = [0u8; 2 + 65536];
    let udp_socket = &context.shared.udp_socket;
    loop {
        eprintln!("recv about to be called");
        let recv_result = udp_socket.recv(&mut buf[2..]).await;
        match recv_result {
            Ok(size) => {
                eprintln!("recv {} bytes", size);
            }
            Err(e) => {
                eprintln!("recv err {:?}", e);
                break;
            }
        }
    }
}
