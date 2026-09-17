#[cfg(feature = "urma")]
fn main() {
    if let Err(error) = run() {
        eprintln!("rm_read_probe: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "urma")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::{TcpListener, TcpStream};
    use urma_transport_lab::rm_read_probe::{
        run_rm_read_child, run_rm_read_parent, RmReadProbeConfig,
    };

    let mut args = std::env::args().skip(1);
    let role = args.next().ok_or_else(usage)?;
    let device = args.next().ok_or_else(usage)?;
    let eid_index: u32 = args.next().ok_or_else(usage)?.parse()?;
    let address = args.next().ok_or_else(usage)?;
    let mut config = RmReadProbeConfig::new(device, eid_index);
    if let Some(value) = args.next() {
        config.transfer_bytes = value.parse()?;
    }
    if let Some(value) = args.next() {
        config.chunk_bytes = value.parse()?;
    }
    if let Some(value) = args.next() {
        config.queue_depth = value.parse()?;
    }
    if args.next().is_some() {
        return Err(usage().into());
    }

    let report = match role.as_str() {
        "parent" => {
            let listener = TcpListener::bind(&address)?;
            eprintln!("rm_read_probe parent: listening on {address}");
            let (stream, peer) = listener.accept()?;
            eprintln!("rm_read_probe parent: accepted {peer}");
            run_rm_read_parent(stream, &config)?
        }
        "child" => run_rm_read_child(TcpStream::connect(&address)?, &config)?,
        _ => return Err(usage().into()),
    };
    println!("{}", report.to_json());
    Ok(())
}

#[cfg(feature = "urma")]
fn usage() -> String {
    "usage: rm_read_probe <parent|child> DEVICE EID_INDEX ADDRESS [BYTES=67108864] [CHUNK=1048576] [DEPTH=128]".into()
}

#[cfg(not(feature = "urma"))]
fn main() {
    eprintln!("rm_read_probe requires a Linux UMDK build with --features urma");
    std::process::exit(2);
}
