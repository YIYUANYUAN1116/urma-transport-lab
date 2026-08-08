#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("child: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::{io, net::TcpStream};
    use urma_transport_lab::{oob::child_handshake, JettyConfig, RuntimeConfig, UrmaRuntime};

    let mut args = std::env::args().skip(1);
    let device = args.next().unwrap_or_else(|| "urma0".into());
    let address = args.next().unwrap_or_else(|| "127.0.0.1:19090".into());
    let mut runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))?;
    eprintln!("child: context ready: {:?}", runtime.capability());
    let mut connection = runtime.create_connection(JettyConfig::default())?;

    let stream = TcpStream::connect(&address)?;
    eprintln!("child: connected to parent {address}");
    let session = child_handshake(stream, &mut connection)?;
    println!("child: READY; press Enter to close");
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(())
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("child requires a Linux UMDK build with --features urma");
}
