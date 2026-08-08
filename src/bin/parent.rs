#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("parent: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        net::TcpListener,
        time::{Duration, Instant},
    };
    use urma_transport_lab::{
        oob::parent_handshake, JettyConfig, Message, RuntimeConfig, UrmaRuntime,
    };

    let mut args = std::env::args().skip(1);
    let device = args.next().unwrap_or_else(|| "urma0".into());
    let address = args.next().unwrap_or_else(|| "127.0.0.1:19090".into());
    let rounds: u32 = args.next().map_or(Ok(1), |value| value.parse())?;
    if rounds == 0 {
        return Err("rounds must be non-zero".into());
    }
    let mut runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))?;
    eprintln!("parent: ContextReady");
    let mut connection = runtime.create_connection(JettyConfig::default())?;

    let listener = TcpListener::bind(&address)?;
    eprintln!("parent: listening on {address}");
    let (stream, peer) = listener.accept()?;
    eprintln!("parent: accepted child {peer}");
    let session = parent_handshake(stream, &mut connection)?;
    let started = Instant::now();
    for round in 0..rounds {
        let ping = connection.wait_for_message(Duration::from_secs(5))?;
        ping.validate_ping()?;
        if round + 1 < rounds {
            connection.recv_ready()?;
        }
        connection.send(&Message::pong())?;
        connection.drain_completions(Duration::from_secs(5))?;
    }
    let stats = connection.stats();
    println!(
        "{{\"role\":\"parent\",\"rounds\":{},\"send_post\":{},\"send_cqe\":{},\"recv_cqe\":{},\"payload_ok\":true,\"elapsed_us\":{}}}",
        rounds,
        stats.send_post,
        stats.send_cqe,
        stats.recv_cqe,
        started.elapsed().as_micros()
    );
    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(())
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("parent requires a Linux UMDK build with --features urma");
}
