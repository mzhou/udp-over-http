use std::convert::Infallible;
use std::io::Error as IoError;
use std::net::{AddrParseError, SocketAddr};
use std::sync::Arc;

use bytes::BytesMut;
use clap::Parser;
use hyper::body::{Bytes, Sender as BodySender};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Request, Response, Server};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{
    channel as broadcast_channel, Receiver as BroadcastReceiver, Sender as BroadcastSender,
};
use tokio::{select, spawn};

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
    addr: SocketAddr,
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let (sender, body) = Body::channel();
    let _ = spawn(forward(context.broadcast_sender.subscribe(), sender));
    Ok(Response::new(body))
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

    let (broadcast_sender, _broadcast_receiver) = broadcast_channel(1024);

    let context = AppContext {
        broadcast_sender,
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
    let broadcast_sender = context.broadcast_sender;
    let udp_socket = &context.shared.udp_socket;
    loop {
        let mut buf = BytesMut::zeroed(2 + 65536);
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
