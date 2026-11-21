//! A refused connect reports `ConnectionRefused` on every platform, from a coroutine and from a
//! thread. On macOS the second `connect()` after an asynchronous failure returned EPIPE
//! (`BrokenPipe`); the real error is in `SO_ERROR`.

use std::io;

fn closed_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

#[test]
fn refused_connect_from_a_thread() {
    let error = may::net::TcpStream::connect(closed_port()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused, "{error:?}");
}

#[test]
fn refused_connect_from_a_coroutine() {
    let address = closed_port();
    let kind = may::go!(move || may::net::TcpStream::connect(address).unwrap_err().kind())
        .join()
        .unwrap();
    assert_eq!(kind, io::ErrorKind::ConnectionRefused);
}

#[cfg(feature = "io_timeout")]
#[test]
fn refused_connect_timeout_from_a_coroutine() {
    let address = closed_port();
    let kind = may::go!(move || {
        may::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(5))
            .unwrap_err()
            .kind()
    })
    .join()
    .unwrap();
    assert_eq!(kind, io::ErrorKind::ConnectionRefused);
}
