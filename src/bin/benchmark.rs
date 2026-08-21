use std::{path::PathBuf, str::FromStr};
use urma_transport_lab::{
    run_tcp_child, run_tcp_parent, BenchmarkCase, BenchmarkScenario, BenchmarkTransport,
    FileCompletionPolicy, FileSource, MemorySource, TcpBenchmarkDestination, TcpBenchmarkSource,
    TimingMode,
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Role {
    Parent,
    Child,
}

#[derive(Clone, Copy)]
enum OutputMode {
    Fresh,
    Truncate,
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
    let mut window = 128u32;
    let mut timing_mode = TimingMode::SteadyState;
    let mut completion_policy = FileCompletionPolicy::Buffered;
    let mut data_seed = 0u64;
    let mut listen = String::from("127.0.0.1:19091");
    let mut parent = String::from("127.0.0.1:19091");
    let mut input = None;
    let mut output = None;
    let mut output_mode = OutputMode::Fresh;
    let mut cleanup_output = false;
    let mut device = String::from("urma0");
    let mut eid_index = 0u32;
    let mut urma_profile = String::from("normal");
    let mut urma_post_list = 16usize;
    let mut crc_workers: Option<usize> = None;

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
            "--output-mode" => {
                output_mode = match required_value(&mut args, "--output-mode")?.as_str() {
                    "fresh" => OutputMode::Fresh,
                    "truncate" => OutputMode::Truncate,
                    value => return Err(format!("invalid --output-mode {value:?}").into()),
                }
            }
            "--cleanup-output" => cleanup_output = true,
            "--device" => device = required_value(&mut args, "--device")?,
            "--eid-index" => eid_index = parse_value(&mut args, "--eid-index")?,
            "--urma-profile" => urma_profile = required_value(&mut args, "--urma-profile")?,
            "--urma-post-list" => urma_post_list = parse_value(&mut args, "--urma-post-list")?,
            "--crc-workers" => crc_workers = Some(parse_value(&mut args, "--crc-workers")?),
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
    if cleanup_output && (role != Role::Child || case.scenario != BenchmarkScenario::File) {
        return Err("--cleanup-output requires --role child --scenario file".into());
    }
    #[cfg(feature = "urma")]
    if case.transport == BenchmarkTransport::Urma {
        use urma_transport_lab::{
            run_urma_child_profile_with_crc_workers, run_urma_parent_profile_with_post_list,
            UrmaBenchmarkDestination, UrmaBenchmarkProfile, UrmaBenchmarkSource,
        };
        let profile = match urma_profile.as_str() {
            "normal" => UrmaBenchmarkProfile::Normal,
            "fixed-tx" => UrmaBenchmarkProfile::FixedTx,
            "rx128" => UrmaBenchmarkProfile::Rx128,
            "fixed-tx-rx128" => UrmaBenchmarkProfile::FixedTxRx128,
            "transport-only" => UrmaBenchmarkProfile::TransportOnly,
            "fixed-tx-transport-only" => UrmaBenchmarkProfile::FixedTxTransportOnly,
            value => return Err(format!("invalid --urma-profile {value:?}").into()),
        };
        let result = match role {
            Role::Parent => {
                let source = match case.scenario {
                    BenchmarkScenario::Memory if profile.uses_fixed_tx() => {
                        UrmaBenchmarkSource::fixed_memory(case.transfer_bytes)
                    }
                    BenchmarkScenario::Memory => UrmaBenchmarkSource::Memory(
                        MemorySource::generate(case.transfer_bytes, case.data_seed)?,
                    ),
                    BenchmarkScenario::File => UrmaBenchmarkSource::File(FileSource::from_path(
                        input.ok_or("file Parent requires --input PATH")?,
                    )?),
                };
                eprintln!("benchmark URMA parent: listening on {listen}");
                run_urma_parent_profile_with_post_list(
                    &case,
                    device,
                    eid_index,
                    &listen,
                    source,
                    profile,
                    urma_post_list,
                )?
            }
            Role::Child => {
                let output_path = match case.scenario {
                    BenchmarkScenario::Memory => None,
                    BenchmarkScenario::File => {
                        Some(output.ok_or("file Child requires --output PATH")?)
                    }
                };
                let destination = match case.scenario {
                    BenchmarkScenario::Memory => UrmaBenchmarkDestination::Memory,
                    BenchmarkScenario::File if matches!(output_mode, OutputMode::Fresh) => {
                        UrmaBenchmarkDestination::FreshFile(
                            output_path.clone().expect("file output path was validated"),
                        )
                    }
                    BenchmarkScenario::File => UrmaBenchmarkDestination::File(
                        output_path.clone().expect("file output path was validated"),
                    ),
                };
                eprintln!("benchmark URMA child: connecting to {parent}");
                let result = run_urma_child_profile_with_crc_workers(
                    &case,
                    device,
                    eid_index,
                    &parent,
                    destination,
                    profile,
                    crc_workers,
                )?;
                cleanup_file_if_requested(cleanup_output, output_path.as_deref())?;
                result
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
    let _ = (
        &device,
        eid_index,
        &urma_profile,
        urma_post_list,
        crc_workers,
    );

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
            let output_path = match case.scenario {
                BenchmarkScenario::Memory => None,
                BenchmarkScenario::File => Some(output.ok_or("file Child requires --output PATH")?),
            };
            let destination = match case.scenario {
                BenchmarkScenario::Memory => TcpBenchmarkDestination::Memory,
                BenchmarkScenario::File if matches!(output_mode, OutputMode::Fresh) => {
                    TcpBenchmarkDestination::FreshFile(
                        output_path.clone().expect("file output path was validated"),
                    )
                }
                BenchmarkScenario::File => TcpBenchmarkDestination::File(
                    output_path.clone().expect("file output path was validated"),
                ),
            };
            eprintln!("benchmark child: connecting to {parent}");
            let result = run_tcp_child(&case, &parent, destination)?;
            cleanup_file_if_requested(cleanup_output, output_path.as_deref())?;
            result
        }
    };
    println!("{}", result.to_json_line());
    Ok(())
}

fn cleanup_file_if_requested(
    cleanup: bool,
    path: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if cleanup {
        let path = path.ok_or("cleanup requested without a file output path")?;
        std::fs::remove_file(path)
            .map_err(|error| format!("remove benchmark output {}: {error}", path.display()))?;
    }
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
           --window N                    default: 128\n\
           --timing-mode steady-state|setup-included\n\
                                         default: steady-state\n\
           --completion-policy buffered|durable\n\
                                         default: buffered\n\
           --seed N                      default: 0\n\
           --listen ADDRESS              Parent bind address, default: 127.0.0.1:19091\n\
           --parent ADDRESS              Child target address, default: 127.0.0.1:19091\n\
           --input PATH                  required for file Parent\n\
           --output PATH                 required for file Child\n\
           --output-mode fresh|truncate  file Child creation mode, default: fresh\n\
           --cleanup-output              remove output after successful verification\n\
           --device NAME                 URMA device, default: urma0\n\
           --eid-index N                 URMA EID index, default: 0\n\
           --urma-profile normal|fixed-tx|rx128|fixed-tx-rx128|transport-only|fixed-tx-transport-only\n\
                                         URMA diagnostic profile, default: normal\n\
           --urma-post-list N             linked SEND WRs per provider post, default: 16\n\
           --crc-workers N               Child CRC workers, default: file=4,\n\
                                         memory=affinity CPUs minus one; maximum: 32"
    );
}
