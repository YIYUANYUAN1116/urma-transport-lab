//! Standalone blocking-TCP baselines built on the transport-neutral B0 harness.

use crate::{
    BenchmarkCase, BenchmarkResult, BenchmarkScenario, BenchmarkSink, BenchmarkTimer,
    BenchmarkTransport, CpuUsage, Error, FileCompletionPolicy, FileSink, FileSource,
    IntegrityResult, MemorySink, MemorySource, Result, TimingMode, TimingSample,
};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    path::PathBuf,
    time::Duration,
};

const CONTROL_MAGIC: u32 = 0x4254_4350;
const CONTROL_VERSION: u16 = 1;
const CONTROL_HEADER_LEN: usize = 12;
const MAX_CONTROL_PAYLOAD: usize = 4096;

#[derive(Clone, Debug)]
pub enum TcpBenchmarkSource {
    Memory(MemorySource),
    File(FileSource),
}

impl TcpBenchmarkSource {
    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let (scenario, length) = match self {
            Self::Memory(source) => (BenchmarkScenario::Memory, source.length()),
            Self::File(source) => (BenchmarkScenario::File, source.length()),
        };
        if scenario != case.scenario {
            return Err(invalid("TCP source scenario does not match benchmark case"));
        }
        if length != case.transfer_bytes {
            return Err(invalid(format!(
                "TCP source length {length} does not match case bytes {}",
                case.transfer_bytes
            )));
        }
        Ok(())
    }

    fn length(&self) -> u64 {
        match self {
            Self::Memory(source) => source.length(),
            Self::File(source) => source.length(),
        }
    }

    fn expected_crc32(&self) -> u32 {
        match self {
            Self::Memory(source) => source.expected_crc32(),
            Self::File(source) => source.expected_crc32(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TcpBenchmarkDestination {
    Memory,
    /// Compatibility mode: create the output or truncate and reuse its inode.
    File(PathBuf),
    /// Reproducible benchmark mode: atomically fail if the output exists.
    FreshFile(PathBuf),
}

impl TcpBenchmarkDestination {
    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let scenario = match self {
            Self::Memory => BenchmarkScenario::Memory,
            Self::File(_) | Self::FreshFile(_) => BenchmarkScenario::File,
        };
        if scenario != case.scenario {
            return Err(invalid(
                "TCP destination scenario does not match benchmark case",
            ));
        }
        Ok(())
    }

    fn create_sink(
        &self,
        expected_bytes: u64,
        expected_crc32: u32,
        completion_policy: FileCompletionPolicy,
    ) -> Result<ActiveSink> {
        match self {
            Self::Memory => Ok(ActiveSink::Memory(MemorySink::new(
                expected_bytes,
                expected_crc32,
            ))),
            Self::File(path) => Ok(ActiveSink::File(FileSink::create(
                path,
                expected_bytes,
                expected_crc32,
                completion_policy,
            )?)),
            Self::FreshFile(path) => Ok(ActiveSink::File(FileSink::create_fresh(
                path,
                expected_bytes,
                expected_crc32,
                completion_policy,
            )?)),
        }
    }
}

enum ActiveSink {
    Memory(MemorySink),
    File(FileSink),
}

impl BenchmarkSink for ActiveSink {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Memory(sink) => sink.write_chunk(bytes),
            Self::File(sink) => sink.write_chunk(bytes),
        }
    }

    fn finish(self) -> Result<IntegrityResult> {
        match self {
            Self::Memory(sink) => sink.finish(),
            Self::File(sink) => sink.finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TcpTransportStats {
    pub parent_read_calls: u64,
    pub parent_write_calls: u64,
    pub child_read_calls: u64,
    pub partial_write_count: u64,
    pub sendfile_calls: u64,
    pub partial_sendfile_count: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl TcpTransportStats {
    fn insert_all(self, output: &mut BTreeMap<String, u64>) {
        for (name, value) in [
            ("parent_read_calls", self.parent_read_calls),
            ("parent_write_calls", self.parent_write_calls),
            ("child_read_calls", self.child_read_calls),
            ("partial_write_count", self.partial_write_count),
            ("sendfile_calls", self.sendfile_calls),
            ("partial_sendfile_count", self.partial_sendfile_count),
            ("bytes_sent", self.bytes_sent),
            ("bytes_received", self.bytes_received),
        ] {
            output.insert(name.into(), value);
        }
    }
}

pub fn run_tcp_parent(
    case: &BenchmarkCase,
    listen: impl ToSocketAddrs,
    source: TcpBenchmarkSource,
) -> Result<BenchmarkResult> {
    validate_tcp_case(case)?;
    source.validate(case)?;
    let setup_measurement = if case.timing_mode == TimingMode::SetupIncluded {
        Some(Measurement::start(case.timing_mode)?)
    } else {
        None
    };
    let listener =
        TcpListener::bind(listen).map_err(|error| io_error("bind TCP listener", error))?;
    run_tcp_parent_with_listener(case, listener, source, setup_measurement)
}

pub fn run_tcp_child(
    case: &BenchmarkCase,
    parent: impl ToSocketAddrs,
    destination: TcpBenchmarkDestination,
) -> Result<BenchmarkResult> {
    validate_tcp_case(case)?;
    destination.validate(case)?;
    let setup_measurement = if case.timing_mode == TimingMode::SetupIncluded {
        Some(Measurement::start(case.timing_mode)?)
    } else {
        None
    };
    let mut stream = TcpStream::connect(parent).map_err(|error| io_error("connect TCP", error))?;
    configure_stream(&stream)?;
    let address_family = socket_family(
        stream
            .peer_addr()
            .map_err(|error| io_error("query TCP peer address", error))?,
    );

    write_frame(&mut stream, FrameType::Request, &encode_case(case)?)?;
    let metadata = read_expected_frame(&mut stream, FrameType::Metadata)?;
    let metadata = decode_metadata(&metadata)?;
    if metadata.case_id != case.case_id
        || metadata.expected_bytes != case.transfer_bytes
        || metadata.scenario != case.scenario
    {
        return Err(Error::Protocol(
            "TCP Metadata does not match requested benchmark case".into(),
        ));
    }

    let sink = destination.create_sink(
        metadata.expected_bytes,
        metadata.expected_crc32,
        case.completion_policy,
    )?;
    write_frame(
        &mut stream,
        FrameType::Ready,
        &encode_case_identity(&case.case_id)?,
    )?;
    let start = read_expected_frame(&mut stream, FrameType::Start)?;
    if decode_case_identity(&start)? != case.case_id {
        return Err(Error::Protocol("TCP Start case_id mismatch".into()));
    }

    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };
    let mut stats = TcpTransportStats::default();
    let integrity = receive_exact(
        &mut stream,
        sink,
        metadata.expected_bytes,
        case.chunk_size_usize()?,
        &mut stats,
    )?;
    let (sample, child_cpu) = measurement.finish()?;
    let mut result = BenchmarkResult::from_sample(case, sample, integrity)?;
    result.child_cpu = Some(child_cpu);
    stats.insert_all(&mut result.transport_stats);
    insert_socket_stats(&mut result.transport_stats, address_family);

    let done = DoneMessage {
        case_id: case.case_id.clone(),
        integrity,
        elapsed_ns: result.elapsed_ns,
        child_cpu,
        child_read_calls: stats.child_read_calls,
        bytes_received: stats.bytes_received,
    };
    write_frame(&mut stream, FrameType::Done, &encode_done(&done)?)?;
    Ok(result)
}

fn run_tcp_parent_with_listener(
    case: &BenchmarkCase,
    listener: TcpListener,
    source: TcpBenchmarkSource,
    setup_measurement: Option<Measurement>,
) -> Result<BenchmarkResult> {
    let (mut stream, peer) = listener
        .accept()
        .map_err(|error| io_error("accept TCP", error))?;
    configure_stream(&stream)?;
    let address_family = socket_family(peer);

    let request = read_expected_frame(&mut stream, FrameType::Request)?;
    let remote_case = decode_case(&request)?;
    if &remote_case != case {
        let _ = write_error(&mut stream, "TCP Request does not match Parent case");
        return Err(Error::Protocol(
            "TCP Request does not match Parent benchmark case".into(),
        ));
    }
    let metadata = MetadataMessage {
        case_id: case.case_id.clone(),
        scenario: case.scenario,
        expected_bytes: source.length(),
        expected_crc32: source.expected_crc32(),
    };
    write_frame(
        &mut stream,
        FrameType::Metadata,
        &encode_metadata(&metadata)?,
    )?;
    let ready = read_expected_frame(&mut stream, FrameType::Ready)?;
    if decode_case_identity(&ready)? != case.case_id {
        let _ = write_error(&mut stream, "TCP Ready case_id mismatch");
        return Err(Error::Protocol("TCP Ready case_id mismatch".into()));
    }

    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };
    write_frame(
        &mut stream,
        FrameType::Start,
        &encode_case_identity(&case.case_id)?,
    )?;
    let mut stats = TcpTransportStats::default();
    match case.transport {
        BenchmarkTransport::TcpUserspace => {
            send_userspace(&mut stream, &source, case.chunk_size_usize()?, &mut stats)?
        }
        BenchmarkTransport::TcpSendfile => {
            send_file_with_sendfile(&mut stream, &source, &mut stats)?
        }
        BenchmarkTransport::Urma => unreachable!("validated TCP case"),
    }
    let (parent_sample, parent_cpu) = measurement.finish()?;

    let done_payload = read_expected_frame(&mut stream, FrameType::Done)?;
    let done = decode_done(&done_payload)?;
    validate_done(case, &metadata, &done)?;
    stats.child_read_calls = done.child_read_calls;
    stats.bytes_received = done.bytes_received;

    let child_sample =
        TimingSample::from_duration(case.timing_mode, Duration::from_nanos(done.elapsed_ns));
    let mut result = BenchmarkResult::from_sample(case, child_sample, done.integrity)?;
    result.parent_cpu = Some(parent_cpu);
    result.child_cpu = Some(done.child_cpu);
    stats.insert_all(&mut result.transport_stats);
    result
        .transport_stats
        .insert("parent_elapsed_ns".into(), parent_sample.elapsed_ns()?);
    insert_socket_stats(&mut result.transport_stats, address_family);
    Ok(result)
}

fn validate_tcp_case(case: &BenchmarkCase) -> Result<()> {
    case.validate()?;
    match case.transport {
        BenchmarkTransport::TcpUserspace => Ok(()),
        BenchmarkTransport::TcpSendfile if case.scenario == BenchmarkScenario::File => {
            #[cfg(target_os = "linux")]
            {
                Ok(())
            }
            #[cfg(not(target_os = "linux"))]
            {
                Err(invalid("tcp-sendfile is only supported on Linux"))
            }
        }
        BenchmarkTransport::TcpSendfile => {
            Err(invalid("tcp-sendfile is only valid for the file scenario"))
        }
        BenchmarkTransport::Urma => Err(invalid(
            "B1 TCP runner does not implement the URMA transport",
        )),
    }
}

fn configure_stream(stream: &TcpStream) -> Result<()> {
    stream
        .set_nodelay(true)
        .map_err(|error| io_error("set TCP_NODELAY", error))?;
    stream
        .set_nonblocking(false)
        .map_err(|error| io_error("set blocking TCP mode", error))
}

fn socket_family(address: SocketAddr) -> u64 {
    if address.is_ipv4() {
        4
    } else {
        6
    }
}

fn insert_socket_stats(stats: &mut BTreeMap<String, u64>, address_family: u64) {
    stats.insert("tcp_nodelay".into(), 1);
    stats.insert("blocking_socket".into(), 1);
    stats.insert("socket_buffer_explicit".into(), 0);
    stats.insert("connection_reuse".into(), 0);
    stats.insert("address_family".into(), address_family);
}

fn send_userspace(
    stream: &mut TcpStream,
    source: &TcpBenchmarkSource,
    chunk_size: usize,
    stats: &mut TcpTransportStats,
) -> Result<()> {
    match source {
        TcpBenchmarkSource::Memory(source) => {
            for chunk in source.chunks(chunk_size)? {
                counted_write_all(stream, chunk, stats)?;
            }
        }
        TcpBenchmarkSource::File(source) => {
            let mut input = source.open()?;
            let mut buffer = vec![0u8; chunk_size];
            let mut remaining = source.length();
            while remaining != 0 {
                let amount = usize::try_from(remaining.min(chunk_size as u64))
                    .expect("amount is bounded by usize chunk_size");
                let read = counted_read(
                    &mut input,
                    &mut buffer[..amount],
                    &mut stats.parent_read_calls,
                )?;
                if read == 0 {
                    return Err(Error::Protocol(
                        "benchmark source reached EOF before configured length".into(),
                    ));
                }
                counted_write_all(stream, &buffer[..read], stats)?;
                remaining -= read as u64;
            }
        }
    }
    Ok(())
}

fn counted_read(reader: &mut impl Read, buffer: &mut [u8], calls: &mut u64) -> Result<usize> {
    loop {
        *calls = calls
            .checked_add(1)
            .ok_or_else(|| invalid("TCP read call counter overflow"))?;
        match reader.read(buffer) {
            Ok(read) => return Ok(read),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error("read TCP benchmark bytes", error)),
        }
    }
}

fn counted_write_all(
    writer: &mut impl Write,
    mut bytes: &[u8],
    stats: &mut TcpTransportStats,
) -> Result<()> {
    while !bytes.is_empty() {
        stats.parent_write_calls = stats
            .parent_write_calls
            .checked_add(1)
            .ok_or_else(|| invalid("TCP write call counter overflow"))?;
        match writer.write(bytes) {
            Ok(0) => {
                return Err(Error::Io {
                    operation: "write TCP benchmark bytes",
                    message: "write returned zero".into(),
                })
            }
            Ok(written) => {
                if written < bytes.len() {
                    stats.partial_write_count = stats
                        .partial_write_count
                        .checked_add(1)
                        .ok_or_else(|| invalid("partial write counter overflow"))?;
                }
                stats.bytes_sent = stats
                    .bytes_sent
                    .checked_add(written as u64)
                    .ok_or_else(|| invalid("TCP sent byte counter overflow"))?;
                bytes = &bytes[written..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error("write TCP benchmark bytes", error)),
        }
    }
    Ok(())
}

fn receive_exact<S: BenchmarkSink>(
    reader: &mut impl Read,
    mut sink: S,
    expected_bytes: u64,
    chunk_size: usize,
    stats: &mut TcpTransportStats,
) -> Result<IntegrityResult> {
    let mut buffer = vec![0u8; chunk_size];
    let mut remaining = expected_bytes;
    while remaining != 0 {
        let amount = usize::try_from(remaining.min(chunk_size as u64))
            .expect("amount is bounded by usize chunk_size");
        let read = counted_read(reader, &mut buffer[..amount], &mut stats.child_read_calls)?;
        if read == 0 {
            return Err(Error::Protocol(format!(
                "TCP payload ended with {remaining} bytes remaining"
            )));
        }
        stats.bytes_received = stats
            .bytes_received
            .checked_add(read as u64)
            .ok_or_else(|| invalid("TCP received byte counter overflow"))?;
        sink.write_chunk(&buffer[..read])?;
        remaining -= read as u64;
    }
    sink.finish()
}

#[cfg(target_os = "linux")]
fn send_file_with_sendfile(
    stream: &mut TcpStream,
    source: &TcpBenchmarkSource,
    stats: &mut TcpTransportStats,
) -> Result<()> {
    use std::os::fd::AsRawFd;

    let TcpBenchmarkSource::File(source) = source else {
        return Err(invalid("tcp-sendfile requires a file source"));
    };
    let input = source.open()?;
    let mut offset: libc::off_t = 0;
    let mut remaining = source.length();
    const MAX_SENDFILE_COUNT: u64 = 0x7fff_f000;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(MAX_SENDFILE_COUNT))
            .expect("Linux sendfile count is bounded by usize");
        stats.sendfile_calls = stats
            .sendfile_calls
            .checked_add(1)
            .ok_or_else(|| invalid("sendfile call counter overflow"))?;
        // SAFETY: both descriptors remain valid for this call, offset points to
        // an initialized off_t, and count is bounded by Linux MAX_RW_COUNT.
        let sent =
            unsafe { libc::sendfile(stream.as_raw_fd(), input.as_raw_fd(), &mut offset, count) };
        if sent == 0 {
            return Err(Error::Protocol(
                "sendfile reached EOF before configured length".into(),
            ));
        }
        if sent < 0 {
            let error = io::Error::last_os_error();
            if matches!(
                error.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(io_error("sendfile TCP benchmark bytes", error));
        }
        let sent = sent as usize;
        if sent < count {
            stats.partial_sendfile_count = stats
                .partial_sendfile_count
                .checked_add(1)
                .ok_or_else(|| invalid("partial sendfile counter overflow"))?;
        }
        stats.bytes_sent = stats
            .bytes_sent
            .checked_add(sent as u64)
            .ok_or_else(|| invalid("TCP sent byte counter overflow"))?;
        remaining -= sent as u64;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn send_file_with_sendfile(
    _stream: &mut TcpStream,
    _source: &TcpBenchmarkSource,
    _stats: &mut TcpTransportStats,
) -> Result<()> {
    Err(invalid("tcp-sendfile is only supported on Linux"))
}

#[derive(Clone, Copy, Debug)]
struct CpuSnapshot(CpuUsage);

impl CpuSnapshot {
    #[cfg(not(windows))]
    fn capture() -> Result<Self> {
        // SAFETY: rusage is plain data initialized before the libc call, and
        // getrusage writes it only for the duration of this call.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: usage points to writable storage for one libc::rusage.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return Err(io_error(
                "get process CPU usage",
                io::Error::last_os_error(),
            ));
        }
        Ok(Self(CpuUsage {
            user_us: timeval_us(usage.ru_utime)?,
            system_us: timeval_us(usage.ru_stime)?,
        }))
    }

    #[cfg(windows)]
    fn capture() -> Result<Self> {
        Ok(Self(CpuUsage::default()))
    }

    fn elapsed_since(self, start: Self) -> Result<CpuUsage> {
        Ok(CpuUsage {
            user_us: self
                .0
                .user_us
                .checked_sub(start.0.user_us)
                .ok_or_else(|| invalid("process user CPU time moved backwards"))?,
            system_us: self
                .0
                .system_us
                .checked_sub(start.0.system_us)
                .ok_or_else(|| invalid("process system CPU time moved backwards"))?,
        })
    }
}

#[cfg(not(windows))]
fn timeval_us(value: libc::timeval) -> Result<u64> {
    let seconds =
        u64::try_from(value.tv_sec).map_err(|_| invalid("process CPU seconds are negative"))?;
    let micros = u64::try_from(value.tv_usec)
        .map_err(|_| invalid("process CPU microseconds are negative"))?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|total| total.checked_add(micros))
        .ok_or_else(|| invalid("process CPU time overflow"))
}

struct Measurement {
    timer: BenchmarkTimer,
    cpu: CpuSnapshot,
}

impl Measurement {
    fn start(mode: TimingMode) -> Result<Self> {
        Ok(Self {
            timer: BenchmarkTimer::start(mode),
            cpu: CpuSnapshot::capture()?,
        })
    }

    fn finish(self) -> Result<(TimingSample, CpuUsage)> {
        let cpu = CpuSnapshot::capture()?.elapsed_since(self.cpu)?;
        Ok((self.timer.finish(), cpu))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
enum FrameType {
    Request = 1,
    Metadata = 2,
    Ready = 3,
    Start = 4,
    Done = 5,
    Error = 6,
}

impl TryFrom<u16> for FrameType {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Metadata),
            3 => Ok(Self::Ready),
            4 => Ok(Self::Start),
            5 => Ok(Self::Done),
            6 => Ok(Self::Error),
            _ => Err(Error::Protocol(format!(
                "unknown TCP control frame type {value}"
            ))),
        }
    }
}

fn write_frame(writer: &mut impl Write, frame_type: FrameType, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_CONTROL_PAYLOAD {
        return Err(Error::Protocol("TCP control payload is too large".into()));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| Error::Protocol("TCP control payload exceeds u32".into()))?;
    writer
        .write_all(&CONTROL_MAGIC.to_be_bytes())
        .and_then(|_| writer.write_all(&CONTROL_VERSION.to_be_bytes()))
        .and_then(|_| writer.write_all(&(frame_type as u16).to_be_bytes()))
        .and_then(|_| writer.write_all(&length.to_be_bytes()))
        .and_then(|_| writer.write_all(payload))
        .map_err(|error| io_error("write TCP control frame", error))
}

fn read_frame(reader: &mut impl Read) -> Result<(FrameType, Vec<u8>)> {
    let mut header = [0u8; CONTROL_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .map_err(|error| io_error("read TCP control header", error))?;
    let magic = u32::from_be_bytes(header[0..4].try_into().expect("fixed slice"));
    if magic != CONTROL_MAGIC {
        return Err(Error::Protocol(format!(
            "invalid TCP control magic 0x{magic:08x}"
        )));
    }
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed slice"));
    if version != CONTROL_VERSION {
        return Err(Error::Protocol(format!(
            "unsupported TCP control version {version}"
        )));
    }
    let frame_type = FrameType::try_from(u16::from_be_bytes(
        header[6..8].try_into().expect("fixed slice"),
    ))?;
    let length = u32::from_be_bytes(header[8..12].try_into().expect("fixed slice")) as usize;
    if length > MAX_CONTROL_PAYLOAD {
        return Err(Error::Protocol(format!(
            "TCP control payload length {length} exceeds {MAX_CONTROL_PAYLOAD}"
        )));
    }
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|error| io_error("read TCP control payload", error))?;
    Ok((frame_type, payload))
}

fn read_expected_frame(reader: &mut impl Read, expected: FrameType) -> Result<Vec<u8>> {
    let (frame_type, payload) = read_frame(reader)?;
    if frame_type == FrameType::Error {
        return Err(Error::Protocol(format!(
            "remote TCP benchmark error: {}",
            decode_string_payload(&payload)?
        )));
    }
    if frame_type != expected {
        return Err(Error::Protocol(format!(
            "received TCP control frame {frame_type:?}, expected {expected:?}"
        )));
    }
    Ok(payload)
}

fn write_error(stream: &mut impl Write, message: &str) -> Result<()> {
    write_frame(stream, FrameType::Error, &encode_string_payload(message)?)
}

fn encode_case(case: &BenchmarkCase) -> Result<Vec<u8>> {
    let mut output = encode_case_identity(&case.case_id)?;
    output.extend_from_slice(&case.repeat.to_be_bytes());
    output.push(scenario_wire(case.scenario));
    output.push(transport_wire(case.transport));
    output.push(timing_wire(case.timing_mode));
    output.push(completion_wire(case.completion_policy));
    output.extend_from_slice(&case.transfer_bytes.to_be_bytes());
    output.extend_from_slice(&case.chunk_size.to_be_bytes());
    output.extend_from_slice(&case.window.to_be_bytes());
    output.extend_from_slice(&case.data_seed.to_be_bytes());
    Ok(output)
}

fn decode_case(input: &[u8]) -> Result<BenchmarkCase> {
    let mut cursor = WireCursor::new(input);
    let case_id = cursor.string()?;
    let repeat = cursor.u32()?;
    let scenario = scenario_from_wire(cursor.u8()?)?;
    let transport = transport_from_wire(cursor.u8()?)?;
    let timing = timing_from_wire(cursor.u8()?)?;
    let completion = completion_from_wire(cursor.u8()?)?;
    let bytes = cursor.u64()?;
    let chunk = cursor.u64()?;
    let window = cursor.u32()?;
    let seed = cursor.u64()?;
    cursor.finish()?;
    BenchmarkCase::new(
        case_id, repeat, scenario, transport, bytes, chunk, window, timing, completion, seed,
    )
}

struct MetadataMessage {
    case_id: String,
    scenario: BenchmarkScenario,
    expected_bytes: u64,
    expected_crc32: u32,
}

fn encode_metadata(metadata: &MetadataMessage) -> Result<Vec<u8>> {
    let mut output = encode_case_identity(&metadata.case_id)?;
    output.push(scenario_wire(metadata.scenario));
    output.extend_from_slice(&metadata.expected_bytes.to_be_bytes());
    output.extend_from_slice(&metadata.expected_crc32.to_be_bytes());
    Ok(output)
}

fn decode_metadata(input: &[u8]) -> Result<MetadataMessage> {
    let mut cursor = WireCursor::new(input);
    let metadata = MetadataMessage {
        case_id: cursor.string()?,
        scenario: scenario_from_wire(cursor.u8()?)?,
        expected_bytes: cursor.u64()?,
        expected_crc32: cursor.u32()?,
    };
    cursor.finish()?;
    Ok(metadata)
}

#[derive(Clone, Debug)]
struct DoneMessage {
    case_id: String,
    integrity: IntegrityResult,
    elapsed_ns: u64,
    child_cpu: CpuUsage,
    child_read_calls: u64,
    bytes_received: u64,
}

fn encode_done(done: &DoneMessage) -> Result<Vec<u8>> {
    let mut output = encode_case_identity(&done.case_id)?;
    output.extend_from_slice(&done.integrity.expected_bytes.to_be_bytes());
    output.extend_from_slice(&done.integrity.actual_bytes.to_be_bytes());
    output.extend_from_slice(&done.integrity.expected_crc32.to_be_bytes());
    output.extend_from_slice(&done.integrity.actual_crc32.to_be_bytes());
    output.push(u8::from(done.integrity.length_ok));
    output.push(u8::from(done.integrity.digest_ok));
    output.extend_from_slice(&done.elapsed_ns.to_be_bytes());
    output.extend_from_slice(&done.child_cpu.user_us.to_be_bytes());
    output.extend_from_slice(&done.child_cpu.system_us.to_be_bytes());
    output.extend_from_slice(&done.child_read_calls.to_be_bytes());
    output.extend_from_slice(&done.bytes_received.to_be_bytes());
    Ok(output)
}

fn decode_done(input: &[u8]) -> Result<DoneMessage> {
    let mut cursor = WireCursor::new(input);
    let case_id = cursor.string()?;
    let expected_bytes = cursor.u64()?;
    let actual_bytes = cursor.u64()?;
    let expected_crc32 = cursor.u32()?;
    let actual_crc32 = cursor.u32()?;
    let length_ok = cursor.boolean()?;
    let digest_ok = cursor.boolean()?;
    let done = DoneMessage {
        case_id,
        integrity: IntegrityResult {
            expected_bytes,
            actual_bytes,
            expected_crc32,
            actual_crc32,
            length_ok,
            digest_ok,
        },
        elapsed_ns: cursor.u64()?,
        child_cpu: CpuUsage {
            user_us: cursor.u64()?,
            system_us: cursor.u64()?,
        },
        child_read_calls: cursor.u64()?,
        bytes_received: cursor.u64()?,
    };
    cursor.finish()?;
    Ok(done)
}

fn validate_done(
    case: &BenchmarkCase,
    metadata: &MetadataMessage,
    done: &DoneMessage,
) -> Result<()> {
    if done.case_id != case.case_id
        || done.integrity.expected_bytes != metadata.expected_bytes
        || done.integrity.actual_bytes != metadata.expected_bytes
        || done.integrity.expected_crc32 != metadata.expected_crc32
        || done.integrity.actual_crc32 != metadata.expected_crc32
        || !done.integrity.is_ok()
        || done.bytes_received != metadata.expected_bytes
    {
        return Err(Error::Protocol(
            "Child Done failed TCP length/CRC32/result validation".into(),
        ));
    }
    Ok(())
}

fn encode_case_identity(case_id: &str) -> Result<Vec<u8>> {
    encode_string_payload(case_id)
}

fn decode_case_identity(input: &[u8]) -> Result<String> {
    decode_string_payload(input)
}

fn encode_string_payload(value: &str) -> Result<Vec<u8>> {
    let length = u16::try_from(value.len())
        .map_err(|_| Error::Protocol("TCP control string exceeds u16".into()))?;
    let mut output = Vec::with_capacity(2 + value.len());
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(output)
}

fn decode_string_payload(input: &[u8]) -> Result<String> {
    let mut cursor = WireCursor::new(input);
    let value = cursor.string()?;
    cursor.finish()?;
    Ok(value)
}

fn scenario_wire(value: BenchmarkScenario) -> u8 {
    match value {
        BenchmarkScenario::Memory => 1,
        BenchmarkScenario::File => 2,
    }
}

fn scenario_from_wire(value: u8) -> Result<BenchmarkScenario> {
    match value {
        1 => Ok(BenchmarkScenario::Memory),
        2 => Ok(BenchmarkScenario::File),
        _ => Err(Error::Protocol(format!("unknown TCP scenario {value}"))),
    }
}

fn transport_wire(value: BenchmarkTransport) -> u8 {
    match value {
        BenchmarkTransport::TcpUserspace => 1,
        BenchmarkTransport::TcpSendfile => 2,
        BenchmarkTransport::Urma => 3,
    }
}

fn transport_from_wire(value: u8) -> Result<BenchmarkTransport> {
    match value {
        1 => Ok(BenchmarkTransport::TcpUserspace),
        2 => Ok(BenchmarkTransport::TcpSendfile),
        3 => Ok(BenchmarkTransport::Urma),
        _ => Err(Error::Protocol(format!("unknown TCP transport {value}"))),
    }
}

fn timing_wire(value: TimingMode) -> u8 {
    match value {
        TimingMode::SteadyState => 1,
        TimingMode::SetupIncluded => 2,
    }
}

fn timing_from_wire(value: u8) -> Result<TimingMode> {
    match value {
        1 => Ok(TimingMode::SteadyState),
        2 => Ok(TimingMode::SetupIncluded),
        _ => Err(Error::Protocol(format!("unknown TCP timing mode {value}"))),
    }
}

fn completion_wire(value: FileCompletionPolicy) -> u8 {
    match value {
        FileCompletionPolicy::Buffered => 1,
        FileCompletionPolicy::Durable => 2,
    }
}

fn completion_from_wire(value: u8) -> Result<FileCompletionPolicy> {
    match value {
        1 => Ok(FileCompletionPolicy::Buffered),
        2 => Ok(FileCompletionPolicy::Durable),
        _ => Err(Error::Protocol(format!(
            "unknown TCP completion policy {value}"
        ))),
    }
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> WireCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Error::Protocol("TCP control cursor overflow".into()))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| Error::Protocol("truncated TCP control payload".into()))?;
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn boolean(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::Protocol(format!(
                "invalid TCP control boolean {value}"
            ))),
        }
    }

    fn string(&mut self) -> Result<String> {
        let length = u16::from_be_bytes(self.take(2)?.try_into().expect("fixed slice")) as usize;
        let value = self.take(length)?;
        String::from_utf8(value.to_vec())
            .map_err(|_| Error::Protocol("TCP control string is not UTF-8".into()))
    }

    fn finish(self) -> Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Protocol(
                "TCP control payload has trailing bytes".into(),
            ))
        }
    }
}

fn invalid(detail: impl Into<String>) -> Error {
    Error::InvalidConfiguration(detail.into())
}

fn io_error(operation: &'static str, error: io::Error) -> Error {
    Error::Io {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc32_bytes;
    use std::{
        io::Cursor,
        sync::atomic::{AtomicU64, Ordering},
        thread,
    };

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempPath(PathBuf);

    impl TempPath {
        fn new(label: &str) -> Self {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "urma-transport-lab-b1-{label}-{}-{sequence}",
                std::process::id()
            )))
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn case(
        scenario: BenchmarkScenario,
        transport: BenchmarkTransport,
        bytes: u64,
        chunk: u64,
        timing: TimingMode,
        policy: FileCompletionPolicy,
    ) -> BenchmarkCase {
        BenchmarkCase::new(
            "b1-test", 1, scenario, transport, bytes, chunk, 1, timing, policy, 42,
        )
        .unwrap()
    }

    fn run_pair(
        case: BenchmarkCase,
        source: TcpBenchmarkSource,
        destination: TcpBenchmarkDestination,
    ) -> (BenchmarkResult, BenchmarkResult) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let parent_case = case.clone();
        let parent = thread::spawn(move || {
            let setup = if parent_case.timing_mode == TimingMode::SetupIncluded {
                Some(Measurement::start(parent_case.timing_mode).unwrap())
            } else {
                None
            };
            run_tcp_parent_with_listener(&parent_case, listener, source, setup).unwrap()
        });
        let child = run_tcp_child(&case, address, destination).unwrap();
        (parent.join().unwrap(), child)
    }

    #[test]
    fn control_case_round_trip_and_invalid_frame_rejection() {
        let case = case(
            BenchmarkScenario::Memory,
            BenchmarkTransport::TcpUserspace,
            17,
            8,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
        );
        assert_eq!(decode_case(&encode_case(&case).unwrap()).unwrap(), case);

        let mut frame = Vec::new();
        write_frame(&mut frame, FrameType::Ready, b"ok").unwrap();
        frame[0] ^= 0xff;
        assert!(read_frame(&mut Cursor::new(frame)).is_err());

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&CONTROL_MAGIC.to_be_bytes());
        oversized.extend_from_slice(&CONTROL_VERSION.to_be_bytes());
        oversized.extend_from_slice(&(FrameType::Ready as u16).to_be_bytes());
        oversized.extend_from_slice(&((MAX_CONTROL_PAYLOAD + 1) as u32).to_be_bytes());
        assert!(read_frame(&mut Cursor::new(oversized)).is_err());
    }

    struct PartialWriter {
        max: usize,
        bytes: Vec<u8>,
    }

    impl Write for PartialWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let length = bytes.len().min(self.max);
            self.bytes.extend_from_slice(&bytes[..length]);
            Ok(length)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PartialReader {
        inner: Cursor<Vec<u8>>,
        max: usize,
    }

    impl Read for PartialReader {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            let amount = bytes.len().min(self.max);
            self.inner.read(&mut bytes[..amount])
        }
    }

    #[test]
    fn partial_read_and_write_helpers_count_actual_calls() {
        let mut writer = PartialWriter {
            max: 3,
            bytes: Vec::new(),
        };
        let mut stats = TcpTransportStats::default();
        counted_write_all(&mut writer, b"abcdefgh", &mut stats).unwrap();
        assert_eq!(writer.bytes, b"abcdefgh");
        assert_eq!(stats.parent_write_calls, 3);
        assert_eq!(stats.partial_write_count, 2);
        assert_eq!(stats.bytes_sent, 8);

        let payload = b"non-multiple".to_vec();
        let mut reader = PartialReader {
            inner: Cursor::new(payload.clone()),
            max: 2,
        };
        let sink = MemorySink::new(payload.len() as u64, crc32_bytes(&payload));
        let mut stats = TcpTransportStats::default();
        let result = receive_exact(&mut reader, sink, payload.len() as u64, 5, &mut stats).unwrap();
        assert!(result.is_ok());
        assert!(stats.child_read_calls > 1);
        assert_eq!(stats.bytes_received, payload.len() as u64);
    }

    #[test]
    fn receive_reports_crc_length_and_premature_eof_failures() {
        let mut stats = TcpTransportStats::default();
        let mismatch = receive_exact(
            &mut Cursor::new(b"bad"),
            MemorySink::new(3, crc32_bytes(b"good")),
            3,
            2,
            &mut stats,
        )
        .unwrap();
        assert!(mismatch.length_ok);
        assert!(!mismatch.digest_ok);

        let mut stats = TcpTransportStats::default();
        let length = receive_exact(
            &mut Cursor::new(b"abc"),
            MemorySink::new(4, crc32_bytes(b"abc")),
            3,
            2,
            &mut stats,
        )
        .unwrap();
        assert!(!length.length_ok);
        assert!(length.digest_ok);

        let mut stats = TcpTransportStats::default();
        assert!(receive_exact(
            &mut Cursor::new(b"abc"),
            MemorySink::new(4, crc32_bytes(b"abcd")),
            4,
            2,
            &mut stats,
        )
        .is_err());
    }

    #[test]
    fn tcp_memory_small_zero_and_non_multiple_transfers() {
        for (bytes, chunk, timing) in [
            (0, 8, TimingMode::SteadyState),
            (17, 8, TimingMode::SteadyState),
            (4097, 511, TimingMode::SetupIncluded),
        ] {
            let case = case(
                BenchmarkScenario::Memory,
                BenchmarkTransport::TcpUserspace,
                bytes,
                chunk,
                timing,
                FileCompletionPolicy::Buffered,
            );
            let source = MemorySource::generate(bytes, case.data_seed).unwrap();
            let (parent, child) = run_pair(
                case,
                TcpBenchmarkSource::Memory(source),
                TcpBenchmarkDestination::Memory,
            );
            assert!(parent.integrity.is_ok());
            assert!(child.integrity.is_ok());
            assert_eq!(parent.transport_stats["bytes_sent"], bytes);
            assert_eq!(parent.transport_stats["bytes_received"], bytes);
        }
    }

    #[test]
    fn tcp_userspace_file_supports_buffered_and_durable_sinks() {
        for policy in [
            FileCompletionPolicy::Buffered,
            FileCompletionPolicy::Durable,
        ] {
            let input = TempPath::new("userspace-input");
            let output = TempPath::new("userspace-output");
            let source = FileSource::generate(&input.0, 12_345, 42, 1024).unwrap();
            let case = case(
                BenchmarkScenario::File,
                BenchmarkTransport::TcpUserspace,
                source.length(),
                777,
                TimingMode::SteadyState,
                policy,
            );
            let (parent, child) = run_pair(
                case,
                TcpBenchmarkSource::File(source),
                TcpBenchmarkDestination::File(output.0.clone()),
            );
            assert!(parent.integrity.is_ok());
            assert!(child.integrity.is_ok());
            assert_eq!(
                std::fs::read(&input.0).unwrap(),
                std::fs::read(&output.0).unwrap()
            );
            assert!(parent.transport_stats["parent_read_calls"] > 0);
            assert_eq!(parent.transport_stats["sendfile_calls"], 0);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn tcp_sendfile_file_transfer_uses_sendfile_stats() {
        let input = TempPath::new("sendfile-input");
        let output = TempPath::new("sendfile-output");
        let source = FileSource::generate(&input.0, 12_345, 42, 1024).unwrap();
        let case = case(
            BenchmarkScenario::File,
            BenchmarkTransport::TcpSendfile,
            source.length(),
            777,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
        );
        let (parent, child) = run_pair(
            case,
            TcpBenchmarkSource::File(source),
            TcpBenchmarkDestination::File(output.0.clone()),
        );
        assert!(parent.integrity.is_ok());
        assert!(child.integrity.is_ok());
        assert_eq!(
            std::fs::read(&input.0).unwrap(),
            std::fs::read(&output.0).unwrap()
        );
        assert_eq!(parent.transport_stats["parent_read_calls"], 0);
        assert_eq!(parent.transport_stats["parent_write_calls"], 0);
        assert!(parent.transport_stats["sendfile_calls"] > 0);
    }

    #[test]
    fn tcp_runner_rejects_sendfile_memory_and_urma() {
        let sendfile_memory = BenchmarkCase {
            transport: BenchmarkTransport::TcpSendfile,
            ..case(
                BenchmarkScenario::Memory,
                BenchmarkTransport::TcpUserspace,
                1,
                1,
                TimingMode::SteadyState,
                FileCompletionPolicy::Buffered,
            )
        };
        assert!(validate_tcp_case(&sendfile_memory).is_err());

        let urma = BenchmarkCase {
            transport: BenchmarkTransport::Urma,
            ..case(
                BenchmarkScenario::Memory,
                BenchmarkTransport::TcpUserspace,
                1,
                1,
                TimingMode::SteadyState,
                FileCompletionPolicy::Buffered,
            )
        };
        assert!(validate_tcp_case(&urma).is_err());
    }
}
