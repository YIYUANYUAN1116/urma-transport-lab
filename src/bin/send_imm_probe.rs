#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("send_imm_probe: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        collections::{HashMap, HashSet},
        net::{TcpListener, TcpStream},
        thread,
        time::{Duration, Instant},
    };
    use urma_transport_lab::{
        completion::CompletionEvent,
        oob::{child_handshake, parent_handshake},
        send_imm_probe::{
            probe_identity, probe_payload, validate_probe_payload, SEND_IMM_PROBE_MAX_MESSAGES,
            SEND_IMM_PROBE_PAYLOAD_LEN,
        },
        BufferPoolConfig, Error, JettyConfig, RuntimeConfig, UrmaConnection, UrmaRuntime,
    };

    const PROBE_ACK: &[u8] = b"SEND_IMM_PROBE_ACK";

    #[derive(Clone, Copy)]
    enum Role {
        Parent,
        Child,
    }

    let mut args = std::env::args().skip(1);
    let role = match args.next().as_deref() {
        Some("parent") => Role::Parent,
        Some("child") => Role::Child,
        _ => return Err(usage().into()),
    };
    let device = args.next().ok_or_else(usage)?;
    let eid_index: u32 = args.next().ok_or_else(usage)?.parse()?;
    let address = args.next().ok_or_else(usage)?;
    let count: usize = args.next().map_or(Ok(64), |value| value.parse())?;
    if args.next().is_some() || count == 0 || count > SEND_IMM_PROBE_MAX_MESSAGES {
        return Err(usage().into());
    }

    let depth = u32::try_from(count)?;
    let mut runtime_config = RuntimeConfig::new(device, eid_index);
    runtime_config.send_jfc_depth = depth.max(16);
    runtime_config.recv_jfc_depth = depth.max(16);
    runtime_config.buffer_pool = BufferPoolConfig {
        slot_size: SEND_IMM_PROBE_PAYLOAD_LEN,
        tx_slot_count: count,
        rx_slot_count: count,
        alignment: 4096,
        alias_tx_slots: false,
    };
    let mut jetty_config = JettyConfig::default();
    jetty_config.send_depth = depth;
    jetty_config.recv_depth = depth;

    let mut runtime = UrmaRuntime::start(runtime_config)?;
    let mut connection = runtime.create_connection(jetty_config)?;
    match role {
        Role::Parent => {
            let listener = TcpListener::bind(&address)?;
            eprintln!("send_imm_probe parent: listening on {address}");
            let (stream, peer) = listener.accept()?;
            eprintln!("send_imm_probe parent: accepted {peer}");
            let mut session = parent_handshake(stream, &mut connection)?;
            run_parent(&mut connection, &mut session, count)?;
            session.wait_for_peer_close()?;
        }
        Role::Child => {
            let stream = TcpStream::connect(&address)?;
            eprintln!("send_imm_probe child: connected to {address}");
            let mut session = child_handshake(stream, &mut connection)?;
            run_child(&mut connection, &mut session, count)?;
            session.close()?;
        }
    }
    connection.close()?;
    runtime.shutdown()?;
    return Ok(());

    fn usage() -> String {
        "usage: send_imm_probe <parent|child> DEVICE EID_INDEX ADDRESS [MESSAGES=64]".into()
    }

    fn run_parent(
        connection: &mut UrmaConnection<'_>,
        session: &mut urma_transport_lab::oob::OobSession,
        count: usize,
    ) -> urma_transport_lab::Result<()> {
        // The OOB handshake already posts one receive WR.
        for _ in 1..count {
            connection.recv_ready()?;
        }
        session.send_probe_ready()?;

        let mut expected = HashMap::with_capacity(count);
        for ordinal in 0..count {
            let identity = probe_identity(ordinal)?;
            if expected.insert(identity, ordinal).is_some() {
                return Err(Error::Protocol(format!(
                    "probe generated duplicate identity 0x{identity:016x}"
                )));
            }
        }
        let mut seen = HashSet::with_capacity(count);
        let mut rx_slots = HashSet::with_capacity(count);
        let deadline = Instant::now() + Duration::from_secs(30);
        while seen.len() < count {
            if Instant::now() >= deadline {
                return Err(Error::Timeout {
                    operation: "receive SEND_IMM probe completions",
                });
            }
            let events = connection.poll_once()?;
            if events.is_empty() {
                thread::yield_now();
                continue;
            }
            for event in events {
                match event {
                    CompletionEvent::RecvCompleted {
                        slot,
                        bytes,
                        imm_data: Some(identity),
                    } => {
                        let ordinal = expected.remove(&identity).ok_or_else(|| {
                            Error::Protocol(format!(
                                "duplicate or unknown SEND_IMM identity 0x{identity:016x}"
                            ))
                        })?;
                        validate_probe_payload(ordinal, identity, &bytes)?;
                        if !seen.insert(identity) {
                            return Err(Error::Protocol(format!(
                                "duplicate SEND_IMM identity 0x{identity:016x}"
                            )));
                        }
                        if !rx_slots.insert(slot.index()) {
                            return Err(Error::Protocol(format!(
                                "RX slot {} completed more than once",
                                slot.index()
                            )));
                        }
                    }
                    CompletionEvent::RecvCompleted { imm_data: None, .. } => {
                        return Err(Error::Protocol(
                            "probe received ordinary SEND instead of SEND_WITH_IMM".into(),
                        ));
                    }
                    CompletionEvent::SendCompleted { .. } => {
                        return Err(Error::Protocol(
                            "receiver observed an unexpected SEND completion".into(),
                        ));
                    }
                }
            }
        }
        if !expected.is_empty() || rx_slots.len() != count {
            return Err(Error::Protocol(format!(
                "SEND_IMM exact-once check failed: missing={} distinct_rx_slots={}/{}",
                expected.len(),
                rx_slots.len(),
                count
            )));
        }
        // Consume the Child's handshake-posted RX through a normal success
        // completion, rather than relying on shutdown flush semantics.
        connection.send_frame(PROBE_ACK)?;
        connection.drain_completions(Duration::from_secs(30))?;
        let stats = connection.stats();
        println!(
            "{{\"state\":\"passed\",\"role\":\"parent\",\"transportMode\":\"RC\",\"opcode\":\"SEND_WITH_IMM\",\"messages\":{},\"distinctImmediate\":{},\"distinctRxSlots\":{},\"full64BitIdentity\":true,\"payloadBinding\":true,\"recvCqe\":{},\"cqeErrors\":{}}}",
            count,
            seen.len(),
            rx_slots.len(),
            stats.recv_cqe,
            stats.cqe_error
        );
        Ok(())
    }

    fn run_child(
        connection: &mut UrmaConnection<'_>,
        session: &mut urma_transport_lab::oob::OobSession,
        count: usize,
    ) -> urma_transport_lab::Result<()> {
        session.wait_probe_ready()?;
        for ordinal in 0..count {
            let identity = probe_identity(ordinal)?;
            connection.send_frame_imm(&probe_payload(ordinal, identity), identity)?;
        }
        connection.drain_completions(Duration::from_secs(30))?;
        let ack = connection.wait_for_frame(Duration::from_secs(30))?;
        if ack != PROBE_ACK {
            return Err(Error::Protocol("invalid SEND_IMM probe ACK".into()));
        }
        let stats = connection.stats();
        if stats.send_retired != count as u64 || stats.cqe_error != 0 {
            return Err(Error::Protocol(format!(
                "SEND_IMM sender completion mismatch: retired={} expected={} errors={}",
                stats.send_retired, count, stats.cqe_error
            )));
        }
        println!(
            "{{\"state\":\"passed\",\"role\":\"child\",\"transportMode\":\"RC\",\"opcode\":\"SEND_IMM\",\"messages\":{},\"sendRetired\":{},\"cqeErrors\":{}}}",
            count, stats.send_retired, stats.cqe_error
        );
        Ok(())
    }
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("send_imm_probe requires a Linux UMDK build with --features urma");
}
