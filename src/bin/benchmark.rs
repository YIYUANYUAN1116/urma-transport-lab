use std::str::FromStr;
use urma_transport_lab::{
    BenchmarkCase, BenchmarkScenario, BenchmarkTransport, FileCompletionPolicy, TimingMode,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut dry_run = false;
    let mut case_id = String::from("b0-dry-run");
    let mut repeat = 1u32;
    let mut scenario = BenchmarkScenario::Memory;
    let mut transport = BenchmarkTransport::TcpUserspace;
    let mut bytes = 0u64;
    let mut chunk_size = 64 * 1024u64;
    let mut window = 1u32;
    let mut timing_mode = TimingMode::SteadyState;
    let mut completion_policy = FileCompletionPolicy::Buffered;
    let mut data_seed = 0u64;

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--dry-run" => dry_run = true,
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
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => return Err(format!("unknown argument {argument:?}; use --help").into()),
        }
    }

    if !dry_run {
        return Err("B0 only supports --dry-run; no transport data path is implemented".into());
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
    println!("{}", case.to_json_line());
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
        "usage: benchmark --dry-run [OPTIONS]\n\
         \n\
         B0 validates and prints one benchmark case as single-line JSON.\n\
         It does not perform network or URMA transport.\n\
         \n\
         OPTIONS:\n\
           --case-id ID                  default: b0-dry-run\n\
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
           --seed N                      default: 0"
    );
}
