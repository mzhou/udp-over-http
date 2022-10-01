use std::convert::Infallible;
use std::net::{AddrParseError, SocketAddr};

use clap::Parser;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Request, Response, Server};
use thiserror::Error;

#[derive(Clone)]
struct AppContext {}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "[::]:0")]
    http_bind: String,
}

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    AddrParse(#[from] AddrParseError),
    #[error(transparent)]
    Hyper(#[from] HyperError),
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

    let context = AppContext {};

    let make_service = make_service_fn(move |conn: &AddrStream| {
        let context = context.clone();
        let addr = conn.remote_addr();
        let service = service_fn(move |req| handle(context.clone(), addr, req));

        async move { Ok::<_, Infallible>(service) }
    });

    let addr: SocketAddr = args.http_bind.parse()?;

    let server = Server::bind(&addr).serve(make_service);

    server.await?;

    Ok(())
}
