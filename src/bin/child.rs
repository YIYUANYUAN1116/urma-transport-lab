#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("child: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        fs::File,
        io::BufWriter,
        net::TcpStream,
        path::PathBuf,
        time::{Duration, Instant},
    };
    use urma_transport_lab::{
        hex_digest, oob::child_handshake, JettyConfig, Message, MessageBody, ReceiveState,
        RuntimeConfig, UrmaConnection, UrmaRuntime,
    };

    enum Mode {
        File(PathBuf),
        PingPong(u32),
    }

    let mut args = std::env::args().skip(1);
    let device = args.next().unwrap_or_else(|| "urma0".into());
    let address = args.next().unwrap_or_else(|| "127.0.0.1:19090".into());
    let mode = match args.next() {
        Some(flag) if flag == "--ping-pong" => {
            let rounds = args.next().map_or(Ok(1), |value| value.parse())?;
            if rounds == 0 {
                return Err("rounds must be non-zero".into());
            }
            Mode::PingPong(rounds)
        }
        Some(path) => Mode::File(PathBuf::from(path)),
        None => {
            return Err("usage: child DEVICE ADDRESS OUTPUT_FILE | --ping-pong [ROUNDS]".into())
        }
    };

    let mut runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))?;
    eprintln!("child: ContextReady");
    let mut connection = runtime.create_connection(JettyConfig::default())?;
    let stream = TcpStream::connect(&address)?;
    eprintln!("child: connected to parent {address}");
    let session = child_handshake(stream, &mut connection)?;
    let started = Instant::now();

    match mode {
        Mode::PingPong(rounds) => run_ping_pong(&mut connection, rounds)?,
        Mode::File(output_path) => {
            const REQUEST_ID: u64 = 1;
            let request = Message::request(REQUEST_ID, "m4-demo-task", 0);
            eprintln!("child: Request request_id={REQUEST_ID} task_id=m4-demo-task piece_number=0");
            connection.send(&request)?;

            let file = File::create(output_path)?;
            let mut output = BufWriter::new(file);
            let mut receiver = ReceiveState::new(REQUEST_ID)?;
            let summary = loop {
                let message = connection.wait_for_message(Duration::from_secs(30))?;
                match &message.body {
                    MessageBody::Metadata {
                        total_length,
                        digest,
                        ..
                    } => {
                        eprintln!(
                            "child: Metadata request_id={} total_length={} digest={}",
                            message.request_id,
                            total_length,
                            hex_digest(digest)
                        );
                        // The CQ poller already copied and released the RX slot.
                        // Repost before any subsequent file I/O.
                        connection.recv_ready()?;
                    }
                    MessageBody::Data(payload) => {
                        eprintln!(
                            "child: Data request_id={} sequence={} bytes={}",
                            message.request_id,
                            message.sequence,
                            payload.len()
                        );
                        connection.recv_ready()?;
                    }
                    MessageBody::End {
                        total_length,
                        chunk_count,
                    } => {
                        eprintln!(
                            "child: End request_id={} total_length={} chunks={}",
                            message.request_id, total_length, chunk_count
                        );
                    }
                    MessageBody::Error {
                        code,
                        message: detail,
                    } => {
                        eprintln!(
                            "child: Error request_id={} code={} message={}",
                            message.request_id, code, detail
                        );
                    }
                    _ => {}
                }
                if let Some(summary) = receiver.accept(&message, &mut output)? {
                    break summary;
                }
            };
            use std::io::Write;
            output.flush()?;
            let stats = connection.stats();
            println!(
                "{{\"role\":\"child\",\"bytes\":{},\"data_messages\":{},\"send_post\":{},\"send_cqe\":{},\"recv_cqe\":{},\"length_ok\":true,\"digest_ok\":true,\"elapsed_us\":{}}}",
                summary.bytes,
                summary.data_messages,
                stats.send_post,
                stats.send_cqe,
                stats.recv_cqe,
                started.elapsed().as_micros()
            );
        }
    }

    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    eprintln!("child: drain/shutdown complete");

    fn run_ping_pong(
        connection: &mut UrmaConnection<'_>,
        rounds: u32,
    ) -> urma_transport_lab::Result<()> {
        for round in 0..rounds {
            connection.send(&Message::ping())?;
            let pong = connection.wait_for_message(Duration::from_secs(5))?;
            pong.validate_pong()?;
            if round + 1 < rounds {
                connection.recv_ready()?;
            }
        }
        let stats = connection.stats();
        println!(
            "{{\"role\":\"child\",\"rounds\":{},\"send_post\":{},\"send_cqe\":{},\"recv_cqe\":{},\"payload_ok\":true}}",
            rounds, stats.send_post, stats.send_cqe, stats.recv_cqe
        );
        Ok(())
    }

    Ok(())
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("child requires a Linux UMDK build with --features urma");
}
