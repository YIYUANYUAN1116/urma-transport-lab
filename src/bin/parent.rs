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
        fs::File,
        io::Read,
        net::TcpListener,
        path::PathBuf,
        time::{Duration, Instant},
    };
    use urma_transport_lab::{
        digest_reader, hex_digest, message::MAX_DATA_PAYLOAD_LEN, oob::parent_handshake,
        JettyConfig, Message, MessageBody, RuntimeConfig, UrmaConnection, UrmaRuntime,
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
            return Err("usage: parent DEVICE ADDRESS INPUT_FILE | --ping-pong [ROUNDS]".into())
        }
    };

    let mut runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))?;
    eprintln!("parent: ContextReady");
    let mut connection = runtime.create_connection(JettyConfig::default())?;
    let listener = TcpListener::bind(&address)?;
    eprintln!("parent: listening on {address}");
    let (stream, peer) = listener.accept()?;
    eprintln!("parent: accepted child {peer}");
    let session = parent_handshake(stream, &mut connection)?;
    let started = Instant::now();

    match mode {
        Mode::PingPong(rounds) => run_ping_pong(&mut connection, rounds)?,
        Mode::File(input_path) => {
            let request = connection.wait_for_message(Duration::from_secs(30))?;
            let (request_id, task_id, piece_number) = match &request.body {
                MessageBody::Request {
                    task_id,
                    piece_number,
                } if request.request_id != 0 && request.sequence == 0 => {
                    (request.request_id, task_id, *piece_number)
                }
                _ => return Err("expected valid Request".into()),
            };
            eprintln!(
                "parent: Request request_id={request_id} task_id={task_id} piece_number={piece_number}"
            );

            let mut digest_file = File::open(&input_path)?;
            let (digest, total_length) = digest_reader(&mut digest_file)?;
            let mut input = File::open(&input_path)?;

            let metadata = Message::metadata(request_id, 0, total_length, digest);
            eprintln!(
                "parent: Metadata request_id={request_id} total_length={total_length} digest={}",
                hex_digest(&digest)
            );
            send_one(&mut connection, &metadata)?;

            let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LEN];
            let mut sequence = 0u32;
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                let data = Message::data(request_id, sequence, buffer[..read].to_vec());
                eprintln!("parent: Data request_id={request_id} sequence={sequence} bytes={read}");
                send_one(&mut connection, &data)?;
                sequence = sequence.checked_add(1).ok_or("Data sequence overflow")?;
            }
            let end = Message::end(request_id, sequence, total_length);
            eprintln!(
                "parent: End request_id={request_id} total_length={total_length} chunks={sequence}"
            );
            send_one(&mut connection, &end)?;

            let stats = connection.stats();
            println!(
                "{{\"role\":\"parent\",\"bytes\":{},\"data_messages\":{},\"send_post\":{},\"send_cqe\":{},\"recv_cqe\":{},\"digest_ok\":true,\"elapsed_us\":{}}}",
                total_length,
                sequence,
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
    eprintln!("parent: drain/shutdown complete");

    fn send_one(
        connection: &mut UrmaConnection<'_>,
        message: &Message,
    ) -> urma_transport_lab::Result<()> {
        connection.send(message)?;
        connection.drain_completions(Duration::from_secs(30))
    }

    fn run_ping_pong(
        connection: &mut UrmaConnection<'_>,
        rounds: u32,
    ) -> urma_transport_lab::Result<()> {
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
            "{{\"role\":\"parent\",\"rounds\":{},\"send_post\":{},\"send_cqe\":{},\"recv_cqe\":{},\"payload_ok\":true}}",
            rounds, stats.send_post, stats.send_cqe, stats.recv_cqe
        );
        Ok(())
    }

    Ok(())
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("parent requires a Linux UMDK build with --features urma");
}
