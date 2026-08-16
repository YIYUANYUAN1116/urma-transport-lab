use std::{path::PathBuf, str::FromStr};
use urma_transport_lab::{
    run_tcp_child, run_tcp_parent, BenchmarkCase, BenchmarkScenario, BenchmarkTransport,
    FileCompletionPolicy, FileSource, MemorySource, TcpBenchmarkDestination, TcpBenchmarkSource,
    TimingMode,
};

enum Role {
    Parent,
    Child,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut dry_run = false;
    let mut role = None;
    let mut case_id = String::from("benchmark-case");
    let mut repeat = 1u32;
    let mut scenario = BenchmarkScenario::Memory;
    let mut transport = BenchmarkTransport::TcpUserspace;
    let mut bytes = 0u64;
    let mut chunk_size = 64 * 1024u64;
    let mut window = 1u32;
    let mut timing_mode = TimingMode::SteadyState;
    let mut completion_policy = FileCompletionPolicy::Buffered;
    let mut data_seed = 0u64;
    let mut listen = String::from("127.0.0.1:19091");
    let mut parent = String::from("127.0.0.1:19091");
    let mut input = None;
    let mut output = None;
    let mut device = String::from("urma0");
    let mut eid_index = 0u32;

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--dry-run" => dry_run = true,
            "--role" => {
                role = Some(match required_value(&mut args, "--role")?.as_str() {
                    "parent" => Role::Parent,
                    "child" => Role::Child,
                    value => return Err(format!("invalid --role {value:?}").into()),
                })
            }
            "--case-id" => case_id = required_value(&mut args, "--case-id")?,
            "--repeat" => repeat = parse_value(&mut args, "--repeat")?,
            "--scenario" => {
                scenario = BenchmarkScenario::from_str(&required_value(&mut args, "--scenario")?)?
            }
            "--transport" => {
                transport =
                    BenchmarkTransport::from_str(&required_value(&mut args, "--transport")?)?
            }
            "--bytes" => bytes = parse_value(&mut args, "--bytes")?,
            "--chunk-size" => chunk_size = parse_value(&mut args, "--chunk-size")?,
            "--window" => window = parse_value(&mut args, "--window")?,
            "--timing-mode" => {
                timing_mode = TimingMode::from_str(&required_value(&mut args, "--timing-mode")?)?
            }
            "--completion-policy" => {
                completion_policy = FileCompletionPolicy::from_str(&required_value(
                    &mut args,
                    "--completion-policy",
                )?)?
            }
            "--seed" => data_seed = parse_value(&mut args, "--seed")?,
            "--listen" => listen = required_value(&mut args, "--listen")?,
            "--parent" => parent = required_value(&mut args, "--parent")?,
            "--input" => input = Some(PathBuf::from(required_value(&mut args, "--input")?)),
            "--output" => output = Some(PathBuf::from(required_value(&mut args, "--output")?)),
            "--device" => device = required_value(&mut args, "--device")?,
            "--eid-index" => eid_index = parse_value(&mut args, "--eid-index")?,
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => return Err(format!("unknown argument {argument:?}; use --help").into()),
        }
    }

    let case = BenchmarkCase::new(
        case_id,
        repeat,
        scenario,
        transport,
        bytes,
        chunk_size,
        window,
        timing_mode,
        completion_policy,
        data_seed,
    )?;
    if dry_run {
        println!("{}", case.to_json_line());
        return Ok(());
    }

    let role = role.ok_or("benchmark requires --role parent or --role child")?;
    #[cfg(feature = "urma")]
    if case.transport == BenchmarkTransport::Urma {
        use urma_transport_lab::{
            run_urma_child, run_urma_parent, UrmaBenchmarkDestination, UrmaBenchmarkSource,
        };
        let result = match role {
            Role::Parent => {
                let source = match case.scenario {
                    BenchmarkScenario::Memory => UrmaBenchmarkSource::Memory(
                        MemorySource::generate(case.transfer_bytes, case.data_seed)?,
                    ),
                    BenchmarkScenario::File => UrmaBenchmarkSource::File(FileSource::from_path(
                        input.ok_or("file Parent requires --input PATH")?,
                    )?),
                };
                eprintln!("benchmark URMA parent: listening on {listen}");
                run_urma_parent(&case, device, eid_index, &listen, source)?
            }
            Role::Child => {
                let destination = match case.scenario {
                    BenchmarkScenario::Memory => UrmaBenchmarkDestination::Memory,
                    BenchmarkScenario::File => UrmaBenchmarkDestination::File(
                        output.ok_or("file Child requires --output PATH")?,
                    ),
                };
                eprintln!("benchmark URMA child: connecting to {parent}");
                run_urma_child(&case, device, eid_index, &parent, destination)?
            }
        };
        println!("{}", result.to_json_line());
        return Ok(());
    }
    #[cfg(not(feature = "urma"))]
    if case.transport == BenchmarkTransport::Urma {
        return Err("URMA benchmark requires --features urma".into());
    }
    #[cfg(not(feature = "urma"))]
    let _ = (&device, eid_index);

    let result = match role {
        Role::Parent => {
            let source = match case.scenario {
                BenchmarkScenario::Memory => TcpBenchmarkSource::Memory(MemorySource::generate(
                    case.transfer_bytes,
                    case.data_seed,
                )?),
                BenchmarkScenario::File => {
                    let path = input.ok_or("file Parent requires --input PATH")?;
                    TcpBenchmarkSource::File(FileSource::from_path(path)?)
                }
            };
            eprintln!("benchmark parent: listening on {listen}");
            run_tcp_parent(&case, &listen, source)?
        }
        Role::Child => {
            let destination = match case.scenario {
                BenchmarkScenario::Memory => TcpBenchmarkDestination::Memory,
                BenchmarkScenario::File => TcpBenchmarkDestination::File(
                    output.ok_or("file Child requires --output PATH")?,
                ),
            };
            eprintln!("benchmark child: connecting to {parent}");
            run_tcp_child(&case, &parent, destination)?
        }
    };
    println!("{}", result.to_json_line());
    Ok(())
}

fn required_value(
    args: &mut impl Iterator<Item = String>,
    option: &'static str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value").into())
}

fn parse_value<T>(
    args: &mut impl Iterator<Item = String>,
    option: &'static str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: FromStr,
    T::Err: std::error::Error + 'static,
{
    let value = required_value(args, option)?;
    value
        .parse::<T>()
        .map_err(|error| format!("invalid value for {option}: {error}").into())
}

fn print_usage() {
    println!(
        "usage: benchmark [--dry-run | --role parent|child] [OPTIONS]\n\
         \n\
         Runs one B1 TCP or B2 URMA case, or validates it with --dry-run.\n\
         \n\
         OPTIONS:\n\
           --role parent|child\n\
           --case-id ID                  default: benchmark-case\n\
           --repeat N                    default: 1\n\
           --scenario memory|file        default: memory\n\
           --transport tcp-userspace|tcp-sendfile|urma\n\
                                         default: tcp-userspace\n\
           --bytes N                     default: 0\n\
           --chunk-size N                default: 65536\n\
           --window N                    default: 1\n\
           --timing-mode steady-state|setup-included\n\
                                         default: steady-state\n\
           --completion-policy buffered|durable\n\
                                         default: buffered\n\
           --seed N                      default: 0\n\
           --listen ADDRESS              Parent bind address, default: 127.0.0.1:19091\n\
           --parent ADDRESS              Child target address, default: 127.0.0.1:19091\n\
           --input PATH                  required for file Parent\n\
           --output PATH                 required for file Child\n\
           --device NAME                 URMA device, default: urma0\n\
           --eid-index N                 URMA EID index, default: 0"
    );
}
