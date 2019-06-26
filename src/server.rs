use super::service::ServiceFactory;
use hyper::rt::{Future, Stream};
use tokio::runtime::TaskExecutor;

pub fn spawn<T: 'static + ServiceFactory + Send>(
    executor: TaskExecutor,
    bind_addr: &std::net::SocketAddr,
    service_factory: T,
) {
    let listener = tokio::net::TcpListener::bind(bind_addr).unwrap();
    let server_fut = listener
        .incoming()
        .for_each({
            let executor = executor.clone();
            move |stream| {
                stream.set_nodelay(false).unwrap();
                stream
                    .set_keepalive(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                stream.set_send_buffer_size(65536).unwrap();
                stream.set_recv_buffer_size(65536).unwrap();
                let http = hyper::server::conn::Http::new();
                let service = service_factory.new_service();
                executor.spawn(http.serve_connection(stream, service).map_err(|_| ()));
                Ok(())
            }
        })
        .map_err(|_| ());
    executor.spawn(server_fut);
}
