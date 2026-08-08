#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("parent: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::TcpListener;
    use urma_transport_lab::{oob::parent_handshake, JettyConfig, RuntimeConfig, UrmaRuntime};

    let mut args = std::env::args().skip(1);
    let device = args.next().unwrap_or_else(|| "urma0".into());
    let address = args.next().unwrap_or_else(|| "127.0.0.1:19090".into());
    let mut runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))?;
    eprintln!("parent: context ready: {:?}", runtime.capability());
    let mut connection = runtime.create_connection(JettyConfig::default())?;

    let listener = TcpListener::bind(&address)?;
    eprintln!("parent: listening on {address}");
    let (stream, peer) = listener.accept()?;
    eprintln!("parent: accepted child {peer}");
    let session = parent_handshake(stream, &mut connection)?;
    println!("parent: READY; waiting for child disconnect");
    session.wait_for_peer_close()?;
    eprintln!("parent: child disconnected; closing Jetty resource tree");
    connection.close()?;
    runtime.shutdown()?;
    Ok(())
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("parent requires a Linux UMDK build with --features urma");
}
