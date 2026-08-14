//! Transport-neutral benchmark case, timing, integrity, source, and sink support.
//!
//! Preparing deterministic data, calculating expected digests, and creating
//! files are deliberately separate from [`BenchmarkTimer`]. A transport starts
//! the timer at its own boundary: after setup for steady-state measurements, or
//! before setup for setup-included measurements.

use crate::{crc32_reader, Crc32Hasher, Error, Result};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};

const DEFAULT_FILE_BUFFER_SIZE: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkScenario {
    Memory,
    File,
}

impl BenchmarkScenario {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
        }
    }
}

impl fmt::Display for BenchmarkScenario {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BenchmarkScenario {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "memory" | "memory-to-memory" => Ok(Self::Memory),
            "file" | "file-to-file" => Ok(Self::File),
            _ => Err(invalid(format!(
                "unknown benchmark scenario {value:?}; expected memory or file"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkTransport {
    TcpUserspace,
    TcpSendfile,
    Urma,
}

impl BenchmarkTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TcpUserspace => "tcp-userspace",
            Self::TcpSendfile => "tcp-sendfile",
            Self::Urma => "urma",
        }
    }
}

impl fmt::Display for BenchmarkTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BenchmarkTransport {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "tcp-userspace" => Ok(Self::TcpUserspace),
            "tcp-sendfile" => Ok(Self::TcpSendfile),
            "urma" => Ok(Self::Urma),
            _ => Err(invalid(format!(
                "unknown benchmark transport {value:?}; expected tcp-userspace, tcp-sendfile, or urma"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimingMode {
    SteadyState,
    SetupIncluded,
}

impl TimingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SteadyState => "steady-state",
            Self::SetupIncluded => "setup-included",
        }
    }
}

impl fmt::Display for TimingMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TimingMode {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "steady-state" => Ok(Self::SteadyState),
            "setup-included" => Ok(Self::SetupIncluded),
            _ => Err(invalid(format!(
                "unknown timing mode {value:?}; expected steady-state or setup-included"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileCompletionPolicy {
    Buffered,
    Durable,
}

impl FileCompletionPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Durable => "durable",
        }
    }
}

impl fmt::Display for FileCompletionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FileCompletionPolicy {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "buffered" => Ok(Self::Buffered),
            "durable" => Ok(Self::Durable),
            _ => Err(invalid(format!(
                "unknown completion policy {value:?}; expected buffered or durable"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkCase {
    pub case_id: String,
    pub repeat: u32,
    pub scenario: BenchmarkScenario,
    pub transport: BenchmarkTransport,
    pub transfer_bytes: u64,
    pub chunk_size: u64,
    pub window: u32,
    pub timing_mode: TimingMode,
    pub completion_policy: FileCompletionPolicy,
    pub data_seed: u64,
}

impl BenchmarkCase {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        case_id: impl Into<String>,
        repeat: u32,
        scenario: BenchmarkScenario,
        transport: BenchmarkTransport,
        transfer_bytes: u64,
        chunk_size: u64,
        window: u32,
        timing_mode: TimingMode,
        completion_policy: FileCompletionPolicy,
        data_seed: u64,
    ) -> Result<Self> {
        let case = Self {
            case_id: case_id.into(),
            repeat,
            scenario,
            transport,
            transfer_bytes,
            chunk_size,
            window,
            timing_mode,
            completion_policy,
            data_seed,
        };
        case.validate()?;
        Ok(case)
    }

    pub fn validate(&self) -> Result<()> {
        if self.case_id.is_empty() || self.case_id.len() > 128 {
            return Err(invalid("case_id length must be in 1..=128"));
        }
        if self.case_id.chars().any(char::is_control) {
            return Err(invalid("case_id must not contain control characters"));
        }
        if self.repeat == 0 {
            return Err(invalid("repeat must be non-zero"));
        }
        if self.chunk_size == 0 {
            return Err(invalid("chunk_size must be non-zero"));
        }
        let chunk_size = usize::try_from(self.chunk_size)
            .map_err(|_| invalid("chunk_size does not fit this platform's usize"))?;
        if chunk_size > isize::MAX as usize {
            return Err(invalid("chunk_size exceeds the maximum Rust buffer size"));
        }
        if self.window == 0 {
            return Err(invalid("window must be non-zero"));
        }
        if self.scenario == BenchmarkScenario::Memory
            && self.transport == BenchmarkTransport::TcpSendfile
        {
            return Err(invalid("tcp-sendfile is only valid for the file scenario"));
        }
        if self.scenario == BenchmarkScenario::Memory
            && self.completion_policy == FileCompletionPolicy::Durable
        {
            return Err(invalid(
                "durable completion is only valid for the file scenario",
            ));
        }
        if self.scenario == BenchmarkScenario::Memory {
            let transfer_bytes = usize::try_from(self.transfer_bytes)
                .map_err(|_| invalid("memory transfer size does not fit this platform's usize"))?;
            if transfer_bytes > isize::MAX as usize {
                return Err(invalid(
                    "memory transfer size exceeds the maximum Rust buffer size",
                ));
            }
        }
        self.chunk_count()?;
        Ok(())
    }

    pub fn chunk_size_usize(&self) -> Result<usize> {
        usize::try_from(self.chunk_size)
            .map_err(|_| invalid("chunk_size does not fit this platform's usize"))
    }

    pub fn chunk_count(&self) -> Result<u64> {
        if self.chunk_size == 0 {
            return Err(invalid("chunk_size must be non-zero"));
        }
        Ok(self.transfer_bytes.div_ceil(self.chunk_size))
    }

    pub fn to_json_line(&self) -> String {
        let mut output = String::with_capacity(320);
        output.push('{');
        json_string_field(&mut output, "case_id", &self.case_id, true);
        json_number_field(&mut output, "repeat", self.repeat, false);
        json_string_field(&mut output, "scenario", self.scenario.as_str(), false);
        json_string_field(&mut output, "transport", self.transport.as_str(), false);
        json_number_field(&mut output, "bytes", self.transfer_bytes, false);
        json_number_field(&mut output, "chunk_size", self.chunk_size, false);
        json_number_field(&mut output, "window", self.window, false);
        json_string_field(&mut output, "timing_mode", self.timing_mode.as_str(), false);
        json_string_field(
            &mut output,
            "completion_policy",
            self.completion_policy.as_str(),
            false,
        );
        json_number_field(&mut output, "data_seed", self.data_seed, false);
        output.push('}');
        output
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntegrityResult {
    pub expected_bytes: u64,
    pub actual_bytes: u64,
    pub expected_crc32: u32,
    pub actual_crc32: u32,
    pub length_ok: bool,
    pub digest_ok: bool,
}

impl IntegrityResult {
    pub fn new(
        expected_bytes: u64,
        actual_bytes: u64,
        expected_crc32: u32,
        actual_crc32: u32,
    ) -> Self {
        Self {
            expected_bytes,
            actual_bytes,
            expected_crc32,
            actual_crc32,
            length_ok: expected_bytes == actual_bytes,
            digest_ok: expected_crc32 == actual_crc32,
        }
    }

    pub const fn is_ok(self) -> bool {
        self.length_ok && self.digest_ok
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuUsage {
    pub user_us: u64,
    pub system_us: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkResult {
    pub case_id: String,
    pub repeat: u32,
    pub transport: BenchmarkTransport,
    pub scenario: BenchmarkScenario,
    pub bytes: u64,
    pub chunk_size: u64,
    pub window: u32,
    pub elapsed_ns: u64,
    pub elapsed_us: u64,
    pub throughput_mib_s: f64,
    pub integrity: IntegrityResult,
    pub timing_mode: TimingMode,
    pub completion_policy: FileCompletionPolicy,
    pub parent_cpu: Option<CpuUsage>,
    pub child_cpu: Option<CpuUsage>,
    pub transport_stats: BTreeMap<String, u64>,
}

impl BenchmarkResult {
    pub fn from_sample(
        case: &BenchmarkCase,
        sample: TimingSample,
        integrity: IntegrityResult,
    ) -> Result<Self> {
        case.validate()?;
        if sample.mode != case.timing_mode {
            return Err(invalid(format!(
                "timing sample mode {} does not match case mode {}",
                sample.mode, case.timing_mode
            )));
        }
        if integrity.expected_bytes != case.transfer_bytes {
            return Err(invalid(format!(
                "integrity expected_bytes {} does not match case transfer_bytes {}",
                integrity.expected_bytes, case.transfer_bytes
            )));
        }
        let elapsed_ns = sample.elapsed_ns()?;
        Ok(Self {
            case_id: case.case_id.clone(),
            repeat: case.repeat,
            transport: case.transport,
            scenario: case.scenario,
            bytes: integrity.actual_bytes,
            chunk_size: case.chunk_size,
            window: case.window,
            elapsed_ns,
            elapsed_us: elapsed_ns / 1_000,
            throughput_mib_s: throughput_mib_s(integrity.actual_bytes, sample.elapsed),
            integrity,
            timing_mode: case.timing_mode,
            completion_policy: case.completion_policy,
            parent_cpu: None,
            child_cpu: None,
            transport_stats: BTreeMap::new(),
        })
    }

    pub fn to_json_line(&self) -> String {
        let mut output = String::with_capacity(640);
        output.push('{');
        json_string_field(&mut output, "case_id", &self.case_id, true);
        json_number_field(&mut output, "repeat", self.repeat, false);
        json_string_field(&mut output, "transport", self.transport.as_str(), false);
        json_string_field(&mut output, "scenario", self.scenario.as_str(), false);
        json_number_field(&mut output, "bytes", self.bytes, false);
        json_number_field(&mut output, "chunk_size", self.chunk_size, false);
        json_number_field(&mut output, "window", self.window, false);
        json_number_field(&mut output, "elapsed_ns", self.elapsed_ns, false);
        json_number_field(&mut output, "elapsed_us", self.elapsed_us, false);
        output.push_str(",\"throughput_mib_s\":");
        output.push_str(&format!("{:.6}", self.throughput_mib_s));
        json_string_field(&mut output, "timing_mode", self.timing_mode.as_str(), false);
        json_string_field(
            &mut output,
            "completion_policy",
            self.completion_policy.as_str(),
            false,
        );
        output.push_str(",\"integrity\":{");
        json_number_field(
            &mut output,
            "expected_bytes",
            self.integrity.expected_bytes,
            true,
        );
        json_number_field(
            &mut output,
            "actual_bytes",
            self.integrity.actual_bytes,
            false,
        );
        json_number_field(
            &mut output,
            "expected_crc32",
            self.integrity.expected_crc32,
            false,
        );
        json_number_field(
            &mut output,
            "actual_crc32",
            self.integrity.actual_crc32,
            false,
        );
        json_bool_field(&mut output, "length_ok", self.integrity.length_ok, false);
        json_bool_field(&mut output, "digest_ok", self.integrity.digest_ok, false);
        json_bool_field(&mut output, "ok", self.integrity.is_ok(), false);
        output.push('}');
        output.push_str(",\"parent_cpu\":");
        json_cpu(&mut output, self.parent_cpu);
        output.push_str(",\"child_cpu\":");
        json_cpu(&mut output, self.child_cpu);
        output.push_str(",\"transport_stats\":{");
        for (index, (name, value)) in self.transport_stats.iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            push_json_string(&mut output, name);
            output.push(':');
            output.push_str(&value.to_string());
        }
        output.push_str("}}");
        output
    }
}

pub fn throughput_mib_s(bytes: u64, elapsed: Duration) -> f64 {
    if bytes == 0 || elapsed.is_zero() {
        return 0.0;
    }
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimingSample {
    pub mode: TimingMode,
    pub elapsed: Duration,
}

impl TimingSample {
    pub const fn from_duration(mode: TimingMode, elapsed: Duration) -> Self {
        Self { mode, elapsed }
    }

    pub fn elapsed_ns(self) -> Result<u64> {
        u64::try_from(self.elapsed.as_nanos())
            .map_err(|_| invalid("elapsed nanoseconds exceed u64"))
    }
}

#[derive(Debug)]
pub struct BenchmarkTimer {
    mode: TimingMode,
    started: Instant,
}

impl BenchmarkTimer {
    pub fn start(mode: TimingMode) -> Self {
        Self {
            mode,
            started: Instant::now(),
        }
    }

    pub fn mode(&self) -> TimingMode {
        self.mode
    }

    pub fn finish(self) -> TimingSample {
        TimingSample {
            mode: self.mode,
            elapsed: self.started.elapsed(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemorySource {
    bytes: Vec<u8>,
    expected_crc32: u32,
}

impl MemorySource {
    pub fn generate(length: u64, seed: u64) -> Result<Self> {
        let length = usize::try_from(length)
            .map_err(|_| invalid("memory source length does not fit this platform's usize"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|error| invalid(format!("cannot allocate memory source: {error}")))?;
        bytes.resize(length, 0);
        DeterministicGenerator::new(seed).fill(&mut bytes);
        let expected_crc32 = crate::crc32_bytes(&bytes);
        Ok(Self {
            bytes,
            expected_crc32,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn chunks(&self, chunk_size: usize) -> Result<std::slice::Chunks<'_, u8>> {
        if chunk_size == 0 {
            return Err(invalid("memory source chunk_size must be non-zero"));
        }
        Ok(self.bytes.chunks(chunk_size))
    }

    pub fn length(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn expected_crc32(&self) -> u32 {
        self.expected_crc32
    }
}

pub trait BenchmarkSink: Sized {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()>;
    fn finish(self) -> Result<IntegrityResult>;
}

#[derive(Debug)]
pub struct MemorySink {
    expected_bytes: u64,
    expected_crc32: u32,
    actual_bytes: u64,
    hasher: Crc32Hasher,
}

impl MemorySink {
    pub fn new(expected_bytes: u64, expected_crc32: u32) -> Self {
        Self {
            expected_bytes,
            expected_crc32,
            actual_bytes: 0,
            hasher: Crc32Hasher::new(),
        }
    }
}

impl BenchmarkSink for MemorySink {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.actual_bytes = self
            .actual_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("memory sink length overflow"))?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn finish(self) -> Result<IntegrityResult> {
        Ok(IntegrityResult::new(
            self.expected_bytes,
            self.actual_bytes,
            self.expected_crc32,
            self.hasher.finalize(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileSource {
    path: PathBuf,
    length: u64,
    expected_crc32: u32,
}

impl FileSource {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file =
            File::open(&path).map_err(|error| io_error("open benchmark source", error))?;
        let (expected_crc32, length) = crc32_reader(&mut file)?;
        Ok(Self {
            path,
            length,
            expected_crc32,
        })
    }

    pub fn generate(
        path: impl AsRef<Path>,
        length: u64,
        seed: u64,
        chunk_size: usize,
    ) -> Result<Self> {
        if chunk_size == 0 {
            return Err(invalid(
                "file source generation chunk_size must be non-zero",
            ));
        }
        let path = path.as_ref().to_path_buf();
        let file =
            File::create(&path).map_err(|error| io_error("create benchmark source", error))?;
        let mut writer = BufWriter::with_capacity(chunk_size, file);
        let mut generator = DeterministicGenerator::new(seed);
        let mut hasher = Crc32Hasher::new();
        let mut remaining = length;
        let mut buffer = vec![0u8; chunk_size];
        while remaining != 0 {
            let amount = usize::try_from(remaining.min(chunk_size as u64))
                .expect("amount is bounded by usize chunk_size");
            generator.fill(&mut buffer[..amount]);
            writer
                .write_all(&buffer[..amount])
                .map_err(|error| io_error("write benchmark source", error))?;
            hasher.update(&buffer[..amount]);
            remaining -= amount as u64;
        }
        writer
            .flush()
            .map_err(|error| io_error("flush benchmark source", error))?;
        Ok(Self {
            path,
            length,
            expected_crc32: hasher.finalize(),
        })
    }

    pub fn open(&self) -> Result<File> {
        File::open(&self.path).map_err(|error| io_error("open benchmark source", error))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn expected_crc32(&self) -> u32 {
        self.expected_crc32
    }
}

#[derive(Debug)]
pub struct FileSink {
    writer: BufWriter<File>,
    expected_bytes: u64,
    expected_crc32: u32,
    actual_bytes: u64,
    hasher: Crc32Hasher,
    completion_policy: FileCompletionPolicy,
}

impl FileSink {
    pub fn create(
        path: impl AsRef<Path>,
        expected_bytes: u64,
        expected_crc32: u32,
        completion_policy: FileCompletionPolicy,
    ) -> Result<Self> {
        Self::create_with_capacity(
            path,
            expected_bytes,
            expected_crc32,
            completion_policy,
            DEFAULT_FILE_BUFFER_SIZE,
        )
    }

    pub fn create_with_capacity(
        path: impl AsRef<Path>,
        expected_bytes: u64,
        expected_crc32: u32,
        completion_policy: FileCompletionPolicy,
        capacity: usize,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(invalid("file sink buffer capacity must be non-zero"));
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error("create benchmark sink", error))?;
        Ok(Self {
            writer: BufWriter::with_capacity(capacity, file),
            expected_bytes,
            expected_crc32,
            actual_bytes: 0,
            hasher: Crc32Hasher::new(),
            completion_policy,
        })
    }
}

impl BenchmarkSink for FileSink {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.actual_bytes = self
            .actual_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("file sink length overflow"))?;
        self.writer
            .write_all(bytes)
            .map_err(|error| io_error("write benchmark sink", error))?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn finish(mut self) -> Result<IntegrityResult> {
        self.writer
            .flush()
            .map_err(|error| io_error("flush benchmark sink", error))?;
        if self.completion_policy == FileCompletionPolicy::Durable {
            self.writer
                .get_ref()
                .sync_data()
                .map_err(|error| io_error("sync benchmark sink data", error))?;
        }
        Ok(IntegrityResult::new(
            self.expected_bytes,
            self.actual_bytes,
            self.expected_crc32,
            self.hasher.finalize(),
        ))
    }
}

#[derive(Debug)]
struct DeterministicGenerator {
    state: u64,
    buffered: [u8; 8],
    position: usize,
}

impl DeterministicGenerator {
    fn new(seed: u64) -> Self {
        Self {
            state: seed,
            buffered: [0; 8],
            position: 8,
        }
    }

    fn fill(&mut self, output: &mut [u8]) {
        for byte in output {
            if self.position == self.buffered.len() {
                self.buffered = self.next_word().to_le_bytes();
                self.position = 0;
            }
            *byte = self.buffered[self.position];
            self.position += 1;
        }
    }
    // SplitMix64 is small, reproducible, and sufficient for benchmark payloads.
    fn next_word(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);

        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

fn invalid(detail: impl Into<String>) -> Error {
    Error::InvalidConfiguration(detail.into())
}

fn io_error(operation: &'static str, error: std::io::Error) -> Error {
    Error::Io {
        operation,
        message: error.to_string(),
    }
}

fn json_cpu(output: &mut String, cpu: Option<CpuUsage>) {
    match cpu {
        Some(cpu) => {
            output.push('{');
            json_number_field(output, "user_us", cpu.user_us, true);
            json_number_field(output, "system_us", cpu.system_us, false);
            output.push('}');
        }
        None => output.push_str("null"),
    }
}

fn json_string_field(output: &mut String, name: &str, value: &str, first: bool) {
    json_field_prefix(output, name, first);
    push_json_string(output, value);
}

fn json_number_field(output: &mut String, name: &str, value: impl fmt::Display, first: bool) {
    json_field_prefix(output, name, first);
    output.push_str(&value.to_string());
}

fn json_bool_field(output: &mut String, name: &str, value: bool, first: bool) {
    json_field_prefix(output, name, first);
    output.push_str(if value { "true" } else { "false" });
}

fn json_field_prefix(output: &mut String, name: &str, first: bool) {
    if !first {
        output.push(',');
    }
    push_json_string(output, name);
    output.push(':');
}

fn push_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            control if control.is_control() => {
                use fmt::Write as _;
                write!(output, "\\u{:04x}", control as u32).expect("write to String");
            }
            other => output.push(other),
        }
    }
    output.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Read,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempPath(PathBuf);

    impl TempPath {
        fn new(label: &str) -> Self {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "urma-transport-lab-b0-{label}-{}-{sequence}",
                std::process::id()
            )))
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn valid_case() -> BenchmarkCase {
        BenchmarkCase::new(
            "b0-case-1",
            1,
            BenchmarkScenario::Memory,
            BenchmarkTransport::TcpUserspace,
            1024,
            256,
            1,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            7,
        )
        .unwrap()
    }

    #[test]
    fn validates_case_and_rejects_invalid_combinations() {
        assert_eq!(valid_case().chunk_count(), Ok(4));
        assert_eq!(
            BenchmarkCase::new(
                "zero",
                1,
                BenchmarkScenario::Memory,
                BenchmarkTransport::Urma,
                0,
                64,
                1,
                TimingMode::SetupIncluded,
                FileCompletionPolicy::Buffered,
                0,
            )
            .unwrap()
            .chunk_count(),
            Ok(0)
        );

        let mut invalid_case = valid_case();
        invalid_case.chunk_size = 0;
        assert!(invalid_case.validate().is_err());
        invalid_case = valid_case();
        invalid_case.window = 0;
        assert!(invalid_case.validate().is_err());
        invalid_case = valid_case();
        invalid_case.repeat = 0;
        assert!(invalid_case.validate().is_err());
        invalid_case = valid_case();
        invalid_case.transport = BenchmarkTransport::TcpSendfile;
        assert!(invalid_case.validate().is_err());
        invalid_case = valid_case();
        invalid_case.completion_policy = FileCompletionPolicy::Durable;
        assert!(invalid_case.validate().is_err());
        invalid_case = valid_case();
        invalid_case.transfer_bytes = u64::MAX;
        assert!(invalid_case.validate().is_err());
    }

    #[test]
    fn chunk_count_avoids_round_up_overflow() {
        let case = BenchmarkCase::new(
            "maximum",
            1,
            BenchmarkScenario::File,
            BenchmarkTransport::TcpUserspace,
            u64::MAX,
            2,
            1,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            0,
        )
        .unwrap();
        assert_eq!(case.chunk_count(), Ok(u64::MAX / 2 + 1));
    }

    #[test]
    fn case_json_is_single_line_stable_and_escaped() {
        let mut case = valid_case();
        case.case_id = "case-\"quoted\"".into();
        assert_eq!(
            case.to_json_line(),
            "{\"case_id\":\"case-\\\"quoted\\\"\",\"repeat\":1,\"scenario\":\"memory\",\"transport\":\"tcp-userspace\",\"bytes\":1024,\"chunk_size\":256,\"window\":1,\"timing_mode\":\"steady-state\",\"completion_policy\":\"buffered\",\"data_seed\":7}"
        );
        assert!(!case.to_json_line().contains('\n'));
    }

    #[test]
    fn throughput_and_result_json_are_stable() {
        let sample = TimingSample::from_duration(TimingMode::SteadyState, Duration::from_secs(2));
        let integrity = IntegrityResult::new(1024 * 1024, 1024 * 1024, 7, 7);
        assert_eq!(throughput_mib_s(1024 * 1024, sample.elapsed), 0.5);
        assert_eq!(throughput_mib_s(0, Duration::ZERO), 0.0);

        let mut case = valid_case();
        case.transfer_bytes = 1024 * 1024;
        let result = BenchmarkResult::from_sample(&case, sample, integrity).unwrap();
        let json = result.to_json_line();
        assert!(json.contains("\"elapsed_ns\":2000000000"));
        assert!(json.contains("\"throughput_mib_s\":0.500000"));
        assert!(json.contains("\"parent_cpu\":null"));
        assert!(json.ends_with("\"transport_stats\":{}}"));
        assert!(!json.contains('\n'));
    }

    #[test]
    fn timer_preserves_explicit_timing_mode() {
        let timer = BenchmarkTimer::start(TimingMode::SetupIncluded);
        assert_eq!(timer.mode(), TimingMode::SetupIncluded);
        let sample = timer.finish();
        assert_eq!(sample.mode, TimingMode::SetupIncluded);
        assert!(sample.elapsed_ns().is_ok());
    }

    #[test]
    fn deterministic_memory_payload_and_crc32_are_reproducible() {
        let first = MemorySource::generate(1025, 42).unwrap();
        let second = MemorySource::generate(1025, 42).unwrap();
        let different = MemorySource::generate(1025, 43).unwrap();
        assert_eq!(first.bytes(), second.bytes());
        assert_ne!(first.bytes(), different.bytes());
        assert_eq!(first.expected_crc32(), crate::crc32_bytes(first.bytes()));

        let mut sink = MemorySink::new(first.length(), first.expected_crc32());
        for chunk in first.chunks(17).unwrap() {
            sink.write_chunk(chunk).unwrap();
        }
        assert!(sink.finish().unwrap().is_ok());
    }

    #[test]
    fn zero_length_memory_payload_is_consumed_and_checked() {
        let source = MemorySource::generate(0, 42).unwrap();
        let sink = MemorySink::new(source.length(), source.expected_crc32());
        let result = sink.finish().unwrap();
        assert!(result.is_ok());
        assert_eq!(result.actual_crc32, 0);
    }

    #[test]
    fn memory_source_reports_impossible_allocation() {
        assert!(MemorySource::generate(u64::MAX, 0).is_err());
    }

    #[test]
    fn generated_file_streams_through_buffered_and_durable_sinks() {
        let source_path = TempPath::new("source");
        let source = FileSource::generate(&source_path.0, 65_537, 99, 4096).unwrap();
        let reopened = FileSource::from_path(&source_path.0).unwrap();
        assert_eq!(source, reopened);
        let memory = MemorySource::generate(source.length(), 99).unwrap();
        let mut source_bytes = Vec::new();
        source
            .open()
            .unwrap()
            .read_to_end(&mut source_bytes)
            .unwrap();
        assert_eq!(source_bytes, memory.bytes());

        for policy in [
            FileCompletionPolicy::Buffered,
            FileCompletionPolicy::Durable,
        ] {
            let output_path = TempPath::new(policy.as_str());
            let mut sink = FileSink::create_with_capacity(
                &output_path.0,
                source.length(),
                source.expected_crc32(),
                policy,
                1024,
            )
            .unwrap();
            let mut input = source.open().unwrap();
            let mut buffer = [0u8; 777];
            loop {
                let read = input.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                sink.write_chunk(&buffer[..read]).unwrap();
            }
            assert!(sink.finish().unwrap().is_ok());
            assert_eq!(
                std::fs::metadata(&output_path.0).unwrap().len(),
                source.length()
            );
        }
    }

    #[test]
    fn zero_length_file_works_for_both_completion_policies() {
        for policy in [
            FileCompletionPolicy::Buffered,
            FileCompletionPolicy::Durable,
        ] {
            let output_path = TempPath::new("empty");
            let sink = FileSink::create(&output_path.0, 0, 0, policy).unwrap();
            assert!(sink.finish().unwrap().is_ok());
            assert_eq!(std::fs::metadata(&output_path.0).unwrap().len(), 0);
        }
    }

    #[test]
    fn sink_reports_length_and_digest_mismatch() {
        let mut sink = MemorySink::new(4, crate::crc32_bytes(b"good"));
        sink.write_chunk(b"bad").unwrap();
        let result = sink.finish().unwrap();
        assert!(!result.length_ok);
        assert!(!result.digest_ok);
        assert!(!result.is_ok());
    }

    #[test]
    fn result_rejects_integrity_for_a_different_case_size() {
        let case = valid_case();
        let sample = TimingSample::from_duration(TimingMode::SteadyState, Duration::from_secs(1));
        let integrity = IntegrityResult::new(1, 1, 0, 0);
        assert!(BenchmarkResult::from_sample(&case, sample, integrity).is_err());
    }
}
