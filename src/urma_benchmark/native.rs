use super::*;
use crate::{
    buffer::{PreparedTxBatch, RegisteredRxWindowLease},
    completion::CompletedRecv,
    derive_urma_slot_size,
    oob::{child_handshake, parent_handshake, OobSession},
    BenchmarkResult, BenchmarkScenario, BenchmarkTimer, CompletionEvent, CompletionStats, CpuUsage,
    Crc32Hasher, DigestDescriptor, FileCompletionPolicy, FileSource, IntegrityResult, JettyConfig,
    MemorySource, RuntimeConfig, TimingMode, TimingSample, UrmaConnection, UrmaRuntime,
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::PathBuf,
    ptr::NonNull,
    sync::{
        mpsc::{self, Receiver, Sender, TryRecvError},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const REQUEST_ID: u64 = 1;
const TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_MAGIC: u32 = 0x4252_4d41;
const CONTROL_VERSION: u16 = 3;
const CONTROL_HEADER_LEN: usize = 12;
const MAX_CONTROL_PAYLOAD: usize = 4096;
const READY: u16 = 1;
const START: u16 = 2;
const DONE: u16 = 3;
const CREDIT: u16 = 4;
const SEND_COMPLETION_INTERVAL: usize = 100;
const VERIFIED_RX_POOL_WINDOWS: usize = 32;
const MAX_CRC_WORKERS: usize = 32;

struct MappedFileSource {
    data: NonNull<u8>,
    length: usize,
}

impl MappedFileSource {
    fn map(source: &FileSource) -> Result<Option<Self>> {
        let length = usize::try_from(source.length())
            .map_err(|_| invalid("file mmap length exceeds usize"))?;
        if length == 0 {
            return Ok(None);
        }
        let file = source.open()?;
        let actual = file
            .metadata()
            .map_err(|error| io_error("stat mmap benchmark source", error))?
            .len();
        if actual != source.length() {
            return Err(invalid(format!(
                "benchmark source length changed before mmap: expected {}, got {actual}",
                source.length()
            )));
        }
        // SAFETY: fd is live for mmap, length is non-zero and validated against
        // the file. MAP_PRIVATE/PROT_READ prevents the benchmark from mutating
        // source content through this mapping.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io_error(
                "mmap benchmark source",
                io::Error::last_os_error(),
            ));
        }
        let data = match NonNull::new(raw.cast::<u8>()) {
            Some(data) => data,
            None => {
                // SAFETY: mmap succeeded and raw/length identify that mapping.
                unsafe {
                    libc::munmap(raw, length);
                }
                return Err(invalid("mmap returned a null source address"));
            }
        };
        // Best-effort hints match Dragonfly's finished-piece mmap path. Page
        // faults and actual source reads may still occur in the measured copy path.
        unsafe {
            libc::madvise(raw, length, libc::MADV_SEQUENTIAL);
            libc::madvise(raw, length, libc::MADV_WILLNEED);
        }
        Ok(Some(Self { data, length }))
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: the mapping remains live and read-only for self's lifetime.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.length) }
    }
}

impl Drop for MappedFileSource {
    fn drop(&mut self) {
        // SAFETY: data/length describe the one live mapping owned by self.
        unsafe {
            libc::munmap(self.data.as_ptr().cast(), self.length);
        }
    }
}

fn registered_rx_window_chunks(application_window: usize, rx_slots: usize) -> Result<usize> {
    let upper = application_window.min(rx_slots);
    (1..=upper)
        .rev()
        .find(|candidate| rx_slots % candidate == 0)
        .ok_or_else(|| invalid("cannot partition RX slots into registered windows"))
}

fn bounded_repost_count(posts_needed: usize, free_rx_slots: usize) -> usize {
    posts_needed.min(free_rx_slots)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UrmaBenchmarkProfile {
    #[default]
    Normal,
    /// Reuse one immutable registered TX address for every logical SEND while
    /// preserving distinct WR ownership and end-to-end CRC verification.
    FixedTx,
    Rx128,
    FixedTxRx128,
    /// Time the data plane independently, but retain every registered receive
    /// window until its full CRC has been computed after the transport sample.
    TransportOnly,
    /// Combine immutable registered TX reuse with deferred verification and
    /// full-payload registered RX backing.
    FixedTxTransportOnly,
}

impl UrmaBenchmarkProfile {
    fn wire_id(self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::FixedTx => 1,
            Self::Rx128 => 2,
            Self::FixedTxRx128 => 3,
            Self::TransportOnly => 4,
            Self::FixedTxTransportOnly => 5,
        }
    }

    pub fn uses_fixed_tx(self) -> bool {
        matches!(
            self,
            Self::FixedTx | Self::FixedTxRx128 | Self::FixedTxTransportOnly
        )
    }

    fn transport_only(self) -> bool {
        matches!(self, Self::TransportOnly | Self::FixedTxTransportOnly)
    }

    fn rx_slots(self, case: &BenchmarkCase, child: bool) -> Result<usize> {
        if self.transport_only() && child {
            return usize::try_from(case.chunk_count()?.saturating_add(1))
                .map_err(|_| invalid("transport-only RX slot count exceeds usize"));
        }
        if matches!(self, Self::Rx128 | Self::FixedTxRx128) {
            Ok(128)
        } else if child {
            let window = case.window as usize;
            let pipeline_slots = window
                .checked_mul(VERIFIED_RX_POOL_WINDOWS)
                .ok_or_else(|| invalid("verified RX pool slot count overflow"))?;
            let minimum_slots = 512usize
                .checked_add(window - 1)
                .ok_or_else(|| invalid("verified RX pool rounding overflow"))?
                / window
                * window;
            Ok(pipeline_slots.max(minimum_slots))
        } else {
            Ok(512)
        }
    }
}

#[derive(Clone, Debug)]
pub enum UrmaBenchmarkSource {
    Memory(MemorySource),
    /// Logical memory source for fixed-TX diagnostics. The data plane supplies
    /// one immutable registered chunk, so the full transfer is not materialized.
    FixedMemory {
        length: u64,
    },
    File(FileSource),
}

impl UrmaBenchmarkSource {
    pub fn fixed_memory(length: u64) -> Self {
        Self::FixedMemory { length }
    }

    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let (scenario, length) = match self {
            Self::Memory(source) => (BenchmarkScenario::Memory, source.length()),
            Self::FixedMemory { length } => (BenchmarkScenario::Memory, *length),
            Self::File(source) => (BenchmarkScenario::File, source.length()),
        };
        if scenario != case.scenario || length != case.transfer_bytes {
            return Err(invalid("URMA source does not match benchmark case"));
        }
        Ok(())
    }

    fn expected_crc32(&self) -> Result<u32> {
        match self {
            Self::Memory(source) => Ok(source.expected_crc32()),
            Self::FixedMemory { .. } => Err(invalid(
                "fixed memory source CRC must come from the fixed registered payload",
            )),
            Self::File(source) => Ok(source.expected_crc32()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UrmaBenchmarkDestination {
    Memory,
    File(PathBuf),
}

impl UrmaBenchmarkDestination {
    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let scenario = match self {
            Self::Memory => BenchmarkScenario::Memory,
            Self::File(_) => BenchmarkScenario::File,
        };
        if scenario != case.scenario {
            return Err(invalid("URMA destination does not match benchmark case"));
        }
        Ok(())
    }

    fn create_sink(
        &self,
        expected_bytes: u64,
        expected_crc32: u32,
        policy: FileCompletionPolicy,
    ) -> Result<WindowSink> {
        match self {
            Self::Memory => Ok(WindowSink::memory(expected_bytes, expected_crc32)),
            Self::File(path) => WindowSink::file(path, expected_bytes, expected_crc32, policy),
        }
    }
}

struct WindowSink {
    file: Option<Arc<File>>,
    expected_bytes: u64,
    expected_crc32: u32,
    completion_policy: FileCompletionPolicy,
}

impl WindowSink {
    fn memory(expected_bytes: u64, expected_crc32: u32) -> Self {
        Self {
            file: None,
            expected_bytes,
            expected_crc32,
            completion_policy: FileCompletionPolicy::Buffered,
        }
    }

    fn file(
        path: &PathBuf,
        expected_bytes: u64,
        expected_crc32: u32,
        completion_policy: FileCompletionPolicy,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error("create direct URMA benchmark sink", error))?;
        Ok(Self {
            file: Some(Arc::new(file)),
            expected_bytes,
            expected_crc32,
            completion_policy,
        })
    }

    fn finish(&self, actual_bytes: u64, actual_crc32: u32) -> Result<IntegrityResult> {
        if self.completion_policy == FileCompletionPolicy::Durable {
            if let Some(file) = &self.file {
                file.sync_data()
                    .map_err(|error| io_error("sync direct URMA benchmark sink", error))?;
            }
        }
        Ok(IntegrityResult::new(
            self.expected_bytes,
            actual_bytes,
            self.expected_crc32,
            actual_crc32,
        ))
    }
}

struct WindowJob {
    order: u64,
    position: u64,
    window: RegisteredRxWindowLease,
}

struct WindowOutcome {
    order: u64,
    length: usize,
    digest: Result<Crc32Hasher>,
    window: RegisteredRxWindowLease,
}

/// Hashes independent registered windows on multiple workers. Window digests
/// may complete out of order, but both digest combination and lease retirement
/// happen in wire order. In-order retirement is required because the RX pool
/// reuses each returned contiguous slot run as a future registered window.
struct SinkPipeline {
    sink: WindowSink,
    commands: Vec<Sender<WindowJob>>,
    event: Receiver<WindowOutcome>,
    workers: Vec<JoinHandle<()>>,
    start_gate: Option<Arc<(Mutex<bool>, Condvar)>>,
    next_worker: usize,
    next_order: u64,
    next_combine: u64,
    outstanding: usize,
    pending: BTreeMap<u64, WindowOutcome>,
    combined: Crc32Hasher,
    actual_bytes: u64,
    recycled: Vec<RegisteredRxWindowLease>,
    failure: Option<Error>,
}

impl SinkPipeline {
    fn start(sink: WindowSink, worker_count: usize, defer_processing: bool) -> Result<Self> {
        if worker_count == 0 {
            return Err(invalid("CRC worker count must be non-zero"));
        }
        let (event_tx, event_rx) = mpsc::channel();
        let start_gate = defer_processing.then(|| Arc::new((Mutex::new(false), Condvar::new())));
        let mut commands = Vec::with_capacity(worker_count);
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let (command_tx, command_rx) = mpsc::channel();
            let event = event_tx.clone();
            let file = sink.file.clone();
            let worker_gate = start_gate.clone();
            let worker = thread::Builder::new()
                .name(format!("urma-rx-crc-{index}"))
                .spawn(move || run_window_worker(file, command_rx, event, worker_gate))
                .map_err(|error| io_error("spawn URMA RX CRC worker", error))?;
            commands.push(command_tx);
            workers.push(worker);
        }
        drop(event_tx);
        Ok(Self {
            sink,
            commands,
            event: event_rx,
            workers,
            start_gate,
            next_worker: 0,
            next_order: 0,
            next_combine: 0,
            outstanding: 0,
            pending: BTreeMap::new(),
            combined: Crc32Hasher::new(),
            actual_bytes: 0,
            recycled: Vec::new(),
            failure: None,
        })
    }

    fn push(&mut self, position: u64, window: RegisteredRxWindowLease) -> Result<()> {
        self.collect_recycled()?;
        validate_registered_window_layout(&window)?;
        let end = position
            .checked_add(window.len() as u64)
            .ok_or_else(|| invalid("registered RX window position overflow"))?;
        if end > self.sink.expected_bytes {
            return Err(Error::Protocol(
                "registered RX window exceeds expected sink length".into(),
            ));
        }
        let order = self.next_order;
        self.next_order = self
            .next_order
            .checked_add(1)
            .ok_or_else(|| invalid("registered RX window order overflow"))?;
        let worker = self.next_worker;
        self.next_worker = (self.next_worker + 1) % self.commands.len();
        self.outstanding = self
            .outstanding
            .checked_add(1)
            .ok_or_else(|| invalid("CRC pipeline outstanding overflow"))?;
        if self.commands[worker]
            .send(WindowJob {
                order,
                position,
                window,
            })
            .is_err()
        {
            self.outstanding -= 1;
            return Err(Error::Protocol("URMA RX CRC worker disconnected".into()));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(IntegrityResult, Vec<RegisteredRxWindowLease>)> {
        self.release_workers();
        while self.outstanding != 0 {
            let outcome = self.event.recv().map_err(|_| {
                Error::Protocol("URMA RX CRC workers disconnected with pending windows".into())
            })?;
            self.accept_outcome(outcome);
        }
        self.commands.clear();
        self.join_workers()?;
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        if !self.pending.is_empty() || self.next_combine != self.next_order {
            return Err(Error::Protocol("CRC window result sequence gap".into()));
        }
        let actual_crc32 = std::mem::take(&mut self.combined).finalize();
        let integrity = self.sink.finish(self.actual_bytes, actual_crc32)?;
        Ok((integrity, std::mem::take(&mut self.recycled)))
    }

    fn take_recycled(&mut self) -> Result<Vec<RegisteredRxWindowLease>> {
        self.collect_recycled()?;
        Ok(std::mem::take(&mut self.recycled))
    }

    fn collect_recycled(&mut self) -> Result<()> {
        loop {
            match self.event.try_recv() {
                Ok(outcome) => self.accept_outcome(outcome),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) if self.outstanding == 0 => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(Error::Protocol(
                        "URMA RX CRC workers disconnected with pending windows".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn accept_outcome(&mut self, outcome: WindowOutcome) {
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.pending.insert(outcome.order, outcome).is_some() && self.failure.is_none() {
            self.failure = Some(Error::Protocol("duplicate CRC window result".into()));
        }
        while let Some(outcome) = self.pending.remove(&self.next_combine) {
            match outcome.digest {
                Ok(digest) => {
                    self.combined.combine(&digest);
                    self.actual_bytes = self.actual_bytes.saturating_add(outcome.length as u64);
                }
                Err(error) if self.failure.is_none() => self.failure = Some(error),
                Err(_) => {}
            }
            // Do not recycle a later physical slot run ahead of an earlier
            // one. Otherwise FIFO RX allocation preserves the worker finish
            // order rather than receive order and the next window can span
            // unrelated registered ranges.
            self.recycled.push(outcome.window);
            self.next_combine = self.next_combine.saturating_add(1);
        }
    }

    fn join_workers(&mut self) -> Result<()> {
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| Error::Protocol("URMA RX CRC worker panicked".into()))?;
        }
        Ok(())
    }

    fn release_workers(&mut self) {
        let Some(gate) = self.start_gate.take() else {
            return;
        };
        let (started, ready) = &*gate;
        if let Ok(mut started) = started.lock() {
            *started = true;
            ready.notify_all();
        };
    }
}

impl Drop for SinkPipeline {
    fn drop(&mut self) {
        self.commands.clear();
        self.release_workers();
        let _ = self.join_workers();
    }
}

fn run_window_worker(
    file: Option<Arc<File>>,
    command: Receiver<WindowJob>,
    event: mpsc::Sender<WindowOutcome>,
    start_gate: Option<Arc<(Mutex<bool>, Condvar)>>,
) {
    if let Some(gate) = start_gate {
        let (started, ready) = &*gate;
        let mut started = match started.lock() {
            Ok(started) => started,
            Err(_) => return,
        };
        while !*started {
            started = match ready.wait(started) {
                Ok(started) => started,
                Err(_) => return,
            };
        }
    }
    while let Ok(job) = command.recv() {
        let length = job.window.len();
        let digest = hash_and_write_window(file.as_ref(), &job.window, job.position);
        if event
            .send(WindowOutcome {
                order: job.order,
                length,
                digest,
                window: job.window,
            })
            .is_err()
        {
            return;
        }
    }
}

fn hash_and_write_window(
    file: Option<&Arc<File>>,
    window: &RegisteredRxWindowLease,
    position: u64,
) -> Result<Crc32Hasher> {
    let mut digest = Crc32Hasher::new();
    if let Some(file) = file {
        let write = thread::scope(|scope| {
            let write = scope.spawn(|| -> io::Result<()> {
                let mut part_position = position;
                for bytes in window.parts() {
                    file.write_all_at(bytes, part_position)?;
                    part_position = part_position
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| io::Error::other("RX file position overflow"))?;
                }
                Ok(())
            });
            for bytes in window.parts() {
                digest.update(bytes);
            }
            write.join()
        });
        write
            .map_err(|_| Error::Protocol("direct file writer panicked".into()))?
            .map_err(|error| io_error("pwrite registered RX window", error))?;
    } else {
        for bytes in window.parts() {
            digest.update(bytes);
        }
    }
    Ok(digest)
}

fn validate_registered_window_layout(window: &RegisteredRxWindowLease) -> Result<()> {
    let mut total = 0usize;
    let mut previous_sequence = None;
    for chunk in window.chunks() {
        let sequence = chunk
            .sequence
            .ok_or_else(|| Error::Protocol("registered RX window lacks a sequence".into()))?;
        if let Some(previous) = previous_sequence {
            if sequence != previous + 1 {
                return Err(Error::Protocol(
                    "registered RX window sequence is not contiguous".into(),
                ));
            }
        }
        previous_sequence = Some(sequence);
        total = total
            .checked_add(chunk.length)
            .ok_or_else(|| invalid("registered RX window length overflow"))?;
    }
    if total != window.len() {
        return Err(Error::Protocol(
            "registered RX window chunk lengths do not match its byte range".into(),
        ));
    }
    Ok(())
}

fn crc_worker_count(window_count: usize, requested: Option<usize>) -> Result<usize> {
    let available = thread::available_parallelism().map_or(1, usize::from);
    select_crc_worker_count(window_count, requested, available)
}

fn select_crc_worker_count(
    window_count: usize,
    requested: Option<usize>,
    available_cpus: usize,
) -> Result<usize> {
    let affinity_budget = available_cpus.saturating_sub(1).max(1);
    let maximum = affinity_budget
        .min(MAX_CRC_WORKERS)
        .min(window_count.max(1));
    match requested {
        Some(0) => Err(invalid("CRC worker count must be non-zero")),
        Some(count) if count > MAX_CRC_WORKERS => Err(invalid(format!(
            "CRC worker count {count} exceeds maximum {MAX_CRC_WORKERS}"
        ))),
        Some(count) if count > affinity_budget => Err(invalid(format!(
            "CRC worker count {count} exceeds affinity budget {affinity_budget}; reserve one CPU for RX polling"
        ))),
        Some(count) if count > window_count.max(1) => Err(invalid(format!(
            "CRC worker count {count} exceeds registered window count {}",
            window_count.max(1)
        ))),
        Some(count) => Ok(count),
        None => Ok(maximum),
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UrmaTransportStats {
    pub send_post: u64,
    pub recv_post: u64,
    pub send_post_calls: u64,
    pub recv_post_calls: u64,
    pub send_post_list_max: u64,
    pub recv_post_list_max: u64,
    pub send_cqe: u64,
    pub send_retired: u64,
    pub recv_cqe: u64,
    pub cqe_error: u64,
    pub poll_calls: u64,
    pub empty_polls: u64,
    pub send_jfc_poll_calls: u64,
    pub send_jfc_empty_polls: u64,
    pub recv_jfc_poll_calls: u64,
    pub recv_jfc_empty_polls: u64,
    pub yield_count: u64,
    pub sleep_count: u64,
    pub backoff_sleep_ns: u64,
    pub jfc_rearm_count: u64,
    pub event_wait_count: u64,
    pub event_wakeup_count: u64,
    pub event_timeout_count: u64,
    pub spurious_wakeup_count: u64,
    pub event_wait_ns: u64,
    pub max_event_wait_ns: u64,
    pub max_empty_streak: u64,
    pub nonempty_polls: u64,
    pub completion_batch_total: u64,
    pub avg_poll_batch_milli: u64,
    pub empty_poll_ratio_ppm: u64,
    pub max_completion_poll_gap_ns: u64,
    pub max_outstanding_send: u64,
    pub current_outstanding_send: u64,
    pub current_outstanding_recv: u64,
    pub configured_window: u64,
    pub configured_receive_credit: u64,
    pub slot_size: u64,
    pub effective_payload_size: u64,
    pub tx_slot_count: u64,
    pub rx_slot_count: u64,
    pub total_registered_bytes: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl UrmaTransportStats {
    fn insert_all(self, output: &mut BTreeMap<String, u64>) {
        for (name, value) in [
            ("send_post", self.send_post),
            ("recv_post", self.recv_post),
            ("send_post_calls", self.send_post_calls),
            ("recv_post_calls", self.recv_post_calls),
            ("send_post_list_max", self.send_post_list_max),
            ("recv_post_list_max", self.recv_post_list_max),
            ("send_cqe", self.send_cqe),
            ("send_retired", self.send_retired),
            ("recv_cqe", self.recv_cqe),
            ("cqe_error", self.cqe_error),
            ("poll_calls", self.poll_calls),
            ("empty_polls", self.empty_polls),
            ("send_jfc_poll_calls", self.send_jfc_poll_calls),
            ("send_jfc_empty_polls", self.send_jfc_empty_polls),
            ("recv_jfc_poll_calls", self.recv_jfc_poll_calls),
            ("recv_jfc_empty_polls", self.recv_jfc_empty_polls),
            ("yield_count", self.yield_count),
            ("sleep_count", self.sleep_count),
            ("backoff_sleep_ns", self.backoff_sleep_ns),
            ("jfc_rearm_count", self.jfc_rearm_count),
            ("event_wait_count", self.event_wait_count),
            ("event_wakeup_count", self.event_wakeup_count),
            ("event_timeout_count", self.event_timeout_count),
            ("spurious_wakeup_count", self.spurious_wakeup_count),
            ("event_wait_ns", self.event_wait_ns),
            ("max_event_wait_ns", self.max_event_wait_ns),
            ("max_empty_streak", self.max_empty_streak),
            ("nonempty_polls", self.nonempty_polls),
            ("completion_batch_total", self.completion_batch_total),
            ("avg_poll_batch_milli", self.avg_poll_batch_milli),
            ("empty_poll_ratio_ppm", self.empty_poll_ratio_ppm),
            (
                "max_completion_poll_gap_ns",
                self.max_completion_poll_gap_ns,
            ),
            ("max_outstanding_send", self.max_outstanding_send),
            ("current_outstanding_send", self.current_outstanding_send),
            ("current_outstanding_recv", self.current_outstanding_recv),
            ("configured_window", self.configured_window),
            ("configured_receive_credit", self.configured_receive_credit),
            ("slot_size", self.slot_size),
            ("effective_payload_size", self.effective_payload_size),
            ("tx_slot_count", self.tx_slot_count),
            ("rx_slot_count", self.rx_slot_count),
            ("total_registered_bytes", self.total_registered_bytes),
            ("bytes_sent", self.bytes_sent),
            ("bytes_received", self.bytes_received),
        ] {
            output.insert(name.into(), value);
        }
    }
}

pub fn run_urma_parent(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    listen: impl ToSocketAddrs,
    source: UrmaBenchmarkSource,
) -> Result<BenchmarkResult> {
    run_urma_parent_profile(
        case,
        device,
        eid_index,
        listen,
        source,
        UrmaBenchmarkProfile::Normal,
    )
}

pub fn run_urma_parent_profile(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    listen: impl ToSocketAddrs,
    source: UrmaBenchmarkSource,
    profile: UrmaBenchmarkProfile,
) -> Result<BenchmarkResult> {
    source.validate(case)?;
    if profile.uses_fixed_tx()
        && (case.scenario != BenchmarkScenario::Memory
            || case.transfer_bytes == 0
            || case.transfer_bytes % case.chunk_size != 0)
    {
        return Err(invalid(
            "fixed-tx profile requires a non-empty, chunk-aligned memory case",
        ));
    }
    if matches!(source, UrmaBenchmarkSource::FixedMemory { .. }) && !profile.uses_fixed_tx() {
        return Err(invalid(
            "fixed memory source requires a fixed-tx benchmark profile",
        ));
    }
    let runtime_config = benchmark_runtime_config(case, device, eid_index, profile, false)?;
    let jetty_config = JettyConfig::default();
    let recv_depth = jetty_config.recv_depth as usize;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config),
    )?;
    let setup_measurement = setup_measurement(case.timing_mode)?;
    let listener = TcpListener::bind(listen).map_err(|error| io_error("bind URMA OOB", error))?;
    let mut runtime = UrmaRuntime::start(runtime_config.clone())?;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config)
            .with_provider_max_message_size(runtime.capability().max_msg_size),
    )?;
    let mut connection = runtime.create_connection(jetty_config)?;
    let (stream, _) = listener
        .accept()
        .map_err(|error| io_error("accept URMA OOB", error))?;
    let mut session = parent_handshake(stream, &mut connection)?;
    configure_control_stream(session.stream_mut())?;

    let request = IntegrationMessageV3::decode(&connection.wait_for_frame(TIMEOUT)?)?;
    match &request.body {
        IntegrationMessageBodyV3::Request {
            task_id,
            piece_number,
        } if request.request_id == REQUEST_ID
            && request.sequence == 0
            && task_id == &case.case_id
            && *piece_number == case.repeat => {}
        _ => return Err(Error::Protocol("invalid URMA benchmark Request".into())),
    }
    let fixed_payload = profile
        .uses_fixed_tx()
        .then(|| vec![0x5a; case.chunk_size_usize().expect("case validated")]);
    let expected_crc32 = if let Some(payload) = fixed_payload.as_deref() {
        repeated_payload_crc32(payload, case.chunk_count()?)
    } else {
        source.expected_crc32()?
    };
    let metadata = IntegrationMessageV3::metadata(
        REQUEST_ID,
        0,
        case.transfer_bytes,
        DigestDescriptor::crc32(expected_crc32),
    );
    connection.send_frame(&metadata.encode()?)?;
    connection.drain_completions(TIMEOUT)?;

    let remaining_messages = usize::try_from(case.chunk_count()? + 1)
        .map_err(|_| invalid("receive message count exceeds usize"))?;
    let remote_rx_capacity = profile.rx_slots(case, true)?.min(recv_depth);
    let expected_remote_credit =
        receive_credit_target(case.window as usize, remote_rx_capacity, remaining_messages)?;
    let initial_remote_credit =
        expect_ready(&mut session, &case.case_id, expected_remote_credit, profile)?;
    let mut remote_credit = RemoteReceiveCredit::new(initial_remote_credit)?;
    // Match Dragonfly's finished-piece upload path: establish the read-only
    // mapping before the steady-state sample, then account page faults and
    // window copies while payload is flowing.
    let mapped_file = match &source {
        UrmaBenchmarkSource::File(source) => match MappedFileSource::map(source) {
            Ok(mapped) => mapped,
            Err(error) => {
                eprintln!("benchmark: file mmap unavailable, falling back to pread: {error}");
                None
            }
        },
        _ => None,
    };
    let file_mmap_tx = mapped_file.is_some();
    let payload_poll_start = PayloadPollStats::from_completion(connection.stats());
    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };
    write_control(session.stream_mut(), START, case.case_id.as_bytes())?;
    let mut pipeline = PipelineTracker::new(case.window as usize)?;
    connection
        .configure_send_completion_interval((case.window as usize).min(SEND_COMPLETION_INTERVAL))?;
    if let Some(payload) = fixed_payload.as_deref() {
        connection.prepare_aliased_tx(payload)?;
    }
    let mut bytes_sent = 0u64;
    let mut tx_fill = TxFillStats::default();
    let data_messages = if let Some(payload) = fixed_payload.as_deref() {
        send_fixed_payload(
            payload.len(),
            case.chunk_count()?,
            &mut connection,
            &mut pipeline,
            session.stream_mut(),
            &mut remote_credit,
            &mut bytes_sent,
        )?
    } else {
        send_source(
            &source,
            case.chunk_size_usize()?,
            &mut connection,
            &mut pipeline,
            session.stream_mut(),
            &mut remote_credit,
            &mut bytes_sent,
            &mut tx_fill,
            mapped_file.as_ref(),
        )?
    };
    drain_pipeline(&mut connection, &mut pipeline)?;
    let end = IntegrationMessageV3::end(REQUEST_ID, data_messages, case.transfer_bytes);
    wait_for_remote_credit(session.stream_mut(), &mut remote_credit)?;
    connection.send_frame(&end.encode()?)?;
    remote_credit.consume()?;
    connection.drain_completions(TIMEOUT)?;
    let parent_payload_poll =
        PayloadPollStats::from_completion(connection.stats()).elapsed_since(payload_poll_start);
    if pipeline.current() != 0 || connection.outstanding_send() != 0 {
        return Err(Error::Protocol("URMA pipeline did not fully drain".into()));
    }
    if case.window > 1 && data_messages > 1 && pipeline.maximum() <= 1 {
        return Err(Error::Protocol(
            "configured W>1 but max_outstanding_send did not exceed one".into(),
        ));
    }
    let (parent_sample, parent_cpu) = measurement.finish()?;

    let done = decode_done(&read_done(session.stream_mut(), &mut remote_credit)?)?;
    if done.case_id != case.case_id || !done.integrity.is_ok() {
        return Err(Error::Protocol("invalid URMA benchmark Done".into()));
    }
    let local = connection.stats();
    let stats = combined_stats(
        case,
        &runtime_config,
        local,
        done.completion,
        bytes_sent,
        done.bytes_received,
        initial_remote_credit,
    )?;
    let child_sample =
        TimingSample::from_duration(case.timing_mode, Duration::from_nanos(done.elapsed_ns));
    let mut result = BenchmarkResult::from_sample(case, child_sample, done.integrity)?;
    result.parent_cpu = Some(parent_cpu);
    result.child_cpu = Some(done.child_cpu);
    stats.insert_all(&mut result.transport_stats);
    insert_remote_credit_stats(&mut result, &remote_credit);
    parent_payload_poll.insert(&mut result, "parent");
    done.payload_poll.insert(&mut result, "child");
    result.transport_stats.insert(
        "fixed_tx_profile".into(),
        u64::from(profile.uses_fixed_tx()),
    );
    result.transport_stats.insert(
        "direct_file_tx".into(),
        u64::from(case.scenario == BenchmarkScenario::File),
    );
    result
        .transport_stats
        .insert("file_mmap_tx".into(), u64::from(file_mmap_tx));
    result
        .transport_stats
        .insert("file_pread_calls".into(), tx_fill.pread_calls);
    result
        .transport_stats
        .insert("file_pread_bytes".into(), tx_fill.pread_bytes);
    result
        .transport_stats
        .insert("file_pread_ns".into(), tx_fill.pread_ns);
    result
        .transport_stats
        .insert("file_tx_batch_count".into(), tx_fill.file_batch_count);
    result.transport_stats.insert(
        "file_tx_batch_max_bytes".into(),
        tx_fill.file_batch_max_bytes,
    );
    result
        .transport_stats
        .insert("tx_fill_bytes".into(), tx_fill.fill_bytes);
    result
        .transport_stats
        .insert("tx_fill_ns".into(), tx_fill.fill_ns);
    result
        .transport_stats
        .insert("tx_fill_overlap_batches".into(), tx_fill.overlap_batches);
    result
        .transport_stats
        .insert("tx_ring_windows".into(), tx_fill.ring_windows);
    result
        .transport_stats
        .insert("parent_elapsed_ns".into(), parent_sample.elapsed_ns()?);

    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(result)
}

pub fn run_urma_child(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    parent: impl ToSocketAddrs,
    destination: UrmaBenchmarkDestination,
) -> Result<BenchmarkResult> {
    run_urma_child_profile(
        case,
        device,
        eid_index,
        parent,
        destination,
        UrmaBenchmarkProfile::Normal,
    )
}

pub fn run_urma_child_profile(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    parent: impl ToSocketAddrs,
    destination: UrmaBenchmarkDestination,
    profile: UrmaBenchmarkProfile,
) -> Result<BenchmarkResult> {
    run_urma_child_profile_with_crc_workers(
        case,
        device,
        eid_index,
        parent,
        destination,
        profile,
        None,
    )
}

pub fn run_urma_child_profile_with_crc_workers(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    parent: impl ToSocketAddrs,
    destination: UrmaBenchmarkDestination,
    profile: UrmaBenchmarkProfile,
    crc_workers: Option<usize>,
) -> Result<BenchmarkResult> {
    destination.validate(case)?;
    let runtime_config = benchmark_runtime_config(case, device, eid_index, profile, true)?;
    let jetty_config = JettyConfig::default();
    let recv_depth = jetty_config.recv_depth as usize;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config),
    )?;
    let rx_window_chunks = if profile.transport_only() {
        case.window as usize
    } else {
        registered_rx_window_chunks(
            case.window as usize,
            runtime_config.buffer_pool.rx_slot_count,
        )?
    };
    let registered_window_count = runtime_config.buffer_pool.rx_slot_count / rx_window_chunks;
    let sink_worker_count = crc_worker_count(registered_window_count, crc_workers)?;
    let setup_measurement = setup_measurement(case.timing_mode)?;
    let mut runtime = UrmaRuntime::start(runtime_config.clone())?;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config)
            .with_provider_max_message_size(runtime.capability().max_msg_size),
    )?;
    let mut connection = runtime.create_connection(jetty_config)?;
    let stream = TcpStream::connect(parent).map_err(|error| io_error("connect URMA OOB", error))?;
    let mut session = child_handshake(stream, &mut connection)?;
    configure_control_stream(session.stream_mut())?;

    let request = IntegrationMessageV3::request(REQUEST_ID, case.case_id.clone(), case.repeat);
    connection.send_frame(&request.encode()?)?;
    let metadata = IntegrationMessageV3::decode(&connection.wait_for_frame(TIMEOUT)?)?;
    let expected_crc32 = match &metadata.body {
        IntegrationMessageBodyV3::Metadata { digest, .. }
            if digest.algorithm == DigestAlgorithm::Crc32 =>
        {
            digest
                .value
                .parse::<u32>()
                .map_err(|_| Error::Protocol("invalid Metadata CRC32".into()))?
        }
        _ => return Err(Error::Protocol("expected URMA benchmark Metadata".into())),
    };
    let mut receiver = UrmaReceiveState::new(REQUEST_ID, case.transfer_bytes, expected_crc32)?;
    receiver.accept_metadata(&metadata)?;
    let sink =
        destination.create_sink(case.transfer_bytes, expected_crc32, case.completion_policy)?;
    let mut sink_pipeline = SinkPipeline::start(sink, sink_worker_count, profile.transport_only())?;
    let remaining_messages = usize::try_from(case.chunk_count()? + 1)
        .map_err(|_| invalid("receive message count exceeds usize"))?;
    let credit_target = receive_credit_target(
        case.window as usize,
        recv_depth.min(runtime_config.buffer_pool.rx_slot_count),
        remaining_messages,
    )?;
    let mut credit = ReceiveCreditController::new(credit_target, remaining_messages)?;
    let initial_credit = replenish_credit(&mut connection, &mut credit)?;
    if connection.receive_credit() != credit.current_credit() {
        return Err(Error::Protocol("RX credit accounting mismatch".into()));
    }
    let mut credit_return = RemoteCreditReturn::new(initial_credit)?;
    write_control(
        session.stream_mut(),
        READY,
        &encode_ready(&case.case_id, initial_credit, profile)?,
    )?;
    expect_case_control(&mut session, START, &case.case_id)?;
    let payload_poll_start = PayloadPollStats::from_completion(connection.stats());
    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };
    let mut measurement = Some(measurement);

    let mut last_progress = Instant::now();
    let mut bytes_received = 0u64;
    let expected_data_messages = u32::try_from(case.chunk_count()?)
        .map_err(|_| Error::Protocol("URMA Data sequence count exceeds u32".into()))?;
    let mut received_data_messages = 0u32;
    let mut pending_window = Vec::<CompletedRecv>::with_capacity(rx_window_chunks);
    let end_message = 'receive: loop {
        for lease in sink_pipeline.take_recycled()? {
            connection.recycle_recv_lease(lease)?;
        }
        let reposted = replenish_credit(&mut connection, &mut credit)?;
        if let Some(returned) = credit_return.reposted(reposted)? {
            write_credit(session.stream_mut(), returned)?;
        }

        let completed = connection.poll_recv_leased()?;
        if completed.is_empty() {
            if received_data_messages == expected_data_messages {
                if let Some(returned) = credit_return.flush()? {
                    write_credit(session.stream_mut(), returned)?;
                }
            }
            if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
                log_child_receive_timeout(&connection, &credit);
                return Err(Error::Timeout {
                    operation: "URMA benchmark receive",
                });
            }
            continue;
        }
        last_progress = Instant::now();
        let mut received_end = None;
        for completion in completed {
            credit.completed()?;
            if received_data_messages < expected_data_messages {
                let expected_sequence = u64::from(received_data_messages);
                if completion.sequence != Some(expected_sequence) {
                    return Err(Error::Protocol(format!(
                        "RX completion sequence {:?}, expected {expected_sequence}",
                        completion.sequence
                    )));
                }
                bytes_received = bytes_received
                    .checked_add(u64::from(completion.length))
                    .ok_or_else(|| Error::Protocol("received byte count overflow".into()))?;
                receiver.accept_data_length(received_data_messages, completion.length as usize)?;
                pending_window.push(completion);
                received_data_messages += 1;
                if pending_window.len() == rx_window_chunks
                    || received_data_messages == expected_data_messages
                {
                    let lease = connection.lease_completed_recvs(&pending_window)?;
                    let position = bytes_received
                        .checked_sub(lease.len() as u64)
                        .ok_or_else(|| invalid("registered RX window position underflow"))?;
                    sink_pipeline.push(position, lease)?;
                    pending_window.clear();
                }
            } else {
                if received_end.is_some() {
                    return Err(Error::Protocol(
                        "received payload after URMA benchmark End".into(),
                    ));
                }
                if !pending_window.is_empty() {
                    return Err(Error::Protocol(
                        "URMA End arrived before the data window was dispatched".into(),
                    ));
                }
                let lease = connection.lease_completed_recvs(std::slice::from_ref(&completion))?;
                let message = IntegrationMessageV3::decode(lease.single_span_bytes()?)?;
                connection.recycle_recv_lease(lease)?;
                received_end = Some(message);
            }
        }

        for lease in sink_pipeline.take_recycled()? {
            connection.recycle_recv_lease(lease)?;
        }
        let reposted = replenish_credit(&mut connection, &mut credit)?;
        if let Some(returned) = credit_return.reposted(reposted)? {
            write_credit(session.stream_mut(), returned)?;
        }
        // Data and End share the same RQ. If Data consumed an exact multiple
        // of the batching threshold, a partial repost may be the only credit
        // that lets the parent send End, so it must be flushed here.
        if received_data_messages == expected_data_messages && received_end.is_none() {
            if let Some(returned) = credit_return.flush()? {
                write_credit(session.stream_mut(), returned)?;
            }
        }
        if let Some(message) = received_end {
            break 'receive message;
        }
    };
    let child_payload_poll =
        PayloadPollStats::from_completion(connection.stats()).elapsed_since(payload_poll_start);
    receiver.accept_end(&end_message)?;
    let transport_measurement = if profile.transport_only() {
        Some(
            measurement
                .take()
                .expect("measurement is present")
                .finish()?,
        )
    } else {
        None
    };
    let verification_started = Instant::now();
    let (integrity, recycled) = sink_pipeline.finish()?;
    let post_transport_verification_ns = if profile.transport_only() {
        u64::try_from(verification_started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    } else {
        0
    };
    for lease in recycled {
        connection.recycle_recv_lease(lease)?;
    }
    if credit.remaining_messages() != 0
        || credit.current_credit() != 0
        || connection.outstanding_recv() != 0
    {
        return Err(Error::Protocol(
            "RX credits or slots remain outstanding after End".into(),
        ));
    }
    if !integrity.is_ok() {
        return Err(Error::Protocol(
            "URMA sink integrity verification failed".into(),
        ));
    }
    let (sample, child_cpu) = match transport_measurement {
        Some(measurement) => measurement,
        None => measurement
            .take()
            .expect("measurement is present")
            .finish()?,
    };
    let completion = connection.stats();
    let mut result = BenchmarkResult::from_sample(case, sample, integrity)?;
    result.child_cpu = Some(child_cpu);
    combined_stats(
        case,
        &runtime_config,
        CompletionStats::default(),
        completion,
        0,
        bytes_received,
        credit_target,
    )?
    .insert_all(&mut result.transport_stats);
    result.transport_stats.insert(
        "remote_credit_initial".into(),
        u64::try_from(initial_credit).map_err(|_| invalid("initial credit does not fit u64"))?,
    );
    result.transport_stats.insert(
        "remote_credit_returned".into(),
        u64::try_from(credit_return.returned())
            .map_err(|_| invalid("returned credit does not fit u64"))?,
    );
    result.transport_stats.insert(
        "remote_credit_updates".into(),
        u64::try_from(credit_return.updates())
            .map_err(|_| invalid("credit update count does not fit u64"))?,
    );
    result.transport_stats.insert(
        "remote_credit_pending".into(),
        u64::try_from(credit_return.pending())
            .map_err(|_| invalid("pending credit does not fit u64"))?,
    );
    result
        .transport_stats
        .insert("remote_credit_wait_count".into(), 0);
    result
        .transport_stats
        .insert("remote_credit_wait_ns".into(), 0);
    result
        .transport_stats
        .insert("remote_credit_max_wait_ns".into(), 0);
    result
        .transport_stats
        .insert("remote_credit_consumed".into(), 0);
    result.transport_stats.insert(
        "registered_rx_window_count".into(),
        u64::try_from(registered_window_count)
            .map_err(|_| invalid("registered RX window count does not fit u64"))?,
    );
    result.transport_stats.insert(
        "registered_rx_window_chunks".into(),
        u64::try_from(rx_window_chunks)
            .map_err(|_| invalid("registered RX window chunks do not fit u64"))?,
    );
    result.transport_stats.insert(
        "registered_rx_window_bytes".into(),
        u64::try_from(
            rx_window_chunks
                .checked_mul(runtime_config.buffer_pool.slot_size)
                .ok_or_else(|| invalid("registered RX window byte size overflow"))?,
        )
        .map_err(|_| invalid("registered RX window byte size does not fit u64"))?,
    );
    result.transport_stats.insert("rx_bounce_copy".into(), 0);
    result.transport_stats.insert(
        "parallel_crc_workers".into(),
        u64::try_from(sink_worker_count)
            .map_err(|_| invalid("CRC worker count does not fit u64"))?,
    );
    result
        .transport_stats
        .insert("transport_only".into(), u64::from(profile.transport_only()));
    result.transport_stats.insert(
        "post_transport_verification_ns".into(),
        post_transport_verification_ns,
    );
    result.transport_stats.insert(
        "direct_file_pwrite".into(),
        u64::from(case.scenario == BenchmarkScenario::File),
    );
    child_payload_poll.insert(&mut result, "child");
    let done = Done {
        case_id: case.case_id.clone(),
        integrity,
        elapsed_ns: result.elapsed_ns,
        child_cpu,
        completion,
        payload_poll: child_payload_poll,
        bytes_received,
    };
    write_control(session.stream_mut(), DONE, &encode_done(&done)?)?;

    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(result)
}

fn send_source(
    source: &UrmaBenchmarkSource,
    chunk_size: usize,
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    control: &mut TcpStream,
    remote_credit: &mut RemoteReceiveCredit,
    bytes_sent: &mut u64,
    stats: &mut TxFillStats,
    mapped_file: Option<&MappedFileSource>,
) -> Result<u32> {
    let (mut window_source, source_length, is_file) = match source {
        UrmaBenchmarkSource::Memory(source) => (
            TxWindowSource::Bytes(source.bytes()),
            source.length(),
            false,
        ),
        UrmaBenchmarkSource::FixedMemory { .. } => {
            return Err(invalid(
                "fixed memory source cannot use the materialized source send path",
            ));
        }
        UrmaBenchmarkSource::File(source) => {
            let length = source.length();
            let window_source = if let Some(mapped) = mapped_file {
                TxWindowSource::Bytes(mapped.as_slice())
            } else {
                TxWindowSource::Pread(source.open()?)
            };
            (window_source, length, true)
        }
    };
    send_windowed_source(
        &mut window_source,
        source_length,
        is_file,
        chunk_size,
        connection,
        pipeline,
        control,
        remote_credit,
        bytes_sent,
        stats,
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TxFillStats {
    pread_calls: u64,
    pread_bytes: u64,
    pread_ns: u64,
    file_batch_count: u64,
    file_batch_max_bytes: u64,
    fill_bytes: u64,
    fill_ns: u64,
    overlap_batches: u64,
    ring_windows: u64,
}

enum TxWindowSource<'a> {
    Bytes(&'a [u8]),
    Pread(File),
}

struct PreparedSourceBatch {
    registered: PreparedTxBatch,
    lengths: Vec<usize>,
    bytes: u64,
}

fn repeated_payload_crc32(payload: &[u8], repetitions: u64) -> u32 {
    let mut combined = Crc32Hasher::new();
    let mut repeated_block = Crc32Hasher::new();
    repeated_block.update(payload);
    let mut remaining = repetitions;
    while remaining != 0 {
        if remaining & 1 != 0 {
            combined.combine(&repeated_block);
        }
        remaining >>= 1;
        if remaining != 0 {
            let block = repeated_block.clone();
            repeated_block.combine(&block);
        }
    }
    combined.finalize()
}

fn send_fixed_payload(
    payload_len: usize,
    chunk_count: u64,
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    control: &mut TcpStream,
    remote_credit: &mut RemoteReceiveCredit,
    bytes_sent: &mut u64,
) -> Result<u32> {
    let mut last_progress = Instant::now();
    let count = u32::try_from(chunk_count)
        .map_err(|_| Error::Protocol("URMA sequence count exceeds u32".into()))?;
    for sequence in 0..count {
        while !pipeline.can_post() {
            let completed = poll_send_completions(connection, pipeline)?;
            if completed != 0 {
                last_progress = Instant::now();
            } else if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
                return Err(Error::Timeout {
                    operation: "URMA fixed TX pipeline capacity",
                });
            }
        }
        wait_for_remote_credit(control, remote_credit)?;
        connection.send_prepared_tracked(
            payload_len,
            u64::from(sequence),
            sequence + 1 == count,
        )?;
        remote_credit.consume()?;
        pipeline.posted()?;
        *bytes_sent = bytes_sent
            .checked_add(payload_len as u64)
            .ok_or_else(|| Error::Protocol("sent byte count overflow".into()))?;
    }
    Ok(count)
}

fn tx_batch_lengths(remaining: u64, chunk_size: usize, max_chunks: usize) -> Result<Vec<usize>> {
    if remaining == 0 || chunk_size == 0 || max_chunks == 0 {
        return Err(invalid(
            "file TX batch requires remaining bytes, chunk size, and capacity",
        ));
    }
    let chunk_size_u64 =
        u64::try_from(chunk_size).map_err(|_| invalid("file TX chunk size exceeds u64"))?;
    let count = usize::try_from(remaining.div_ceil(chunk_size_u64).min(max_chunks as u64))
        .map_err(|_| invalid("file TX batch count exceeds usize"))?;
    let mut lengths = vec![chunk_size; count];
    let prefix = chunk_size_u64
        .checked_mul((count - 1) as u64)
        .ok_or_else(|| invalid("file TX batch prefix overflow"))?;
    lengths[count - 1] = usize::try_from((remaining - prefix).min(chunk_size_u64))
        .map_err(|_| invalid("file TX tail length exceeds usize"))?;
    Ok(lengths)
}

fn prepare_source_batch(
    source: &mut TxWindowSource<'_>,
    source_length: u64,
    source_offset: u64,
    is_file: bool,
    chunk_size: usize,
    max_chunks: usize,
    connection: &mut UrmaConnection<'_>,
    stats: &mut TxFillStats,
    overlaps_send: bool,
) -> Result<PreparedSourceBatch> {
    let lengths = tx_batch_lengths(source_length - source_offset, chunk_size, max_chunks)?;
    let batch_bytes = lengths.iter().try_fold(0usize, |total, &length| {
        total
            .checked_add(length)
            .ok_or_else(|| invalid("TX batch byte count overflow"))
    })?;
    let mut pread_calls = 0u64;
    let mut pread_ns = 0u64;
    let mut fill_ns = 0u64;
    let used_pread = matches!(source, TxWindowSource::Pread(_));
    let registered = connection.prepare_filled_batch(&lengths, |registered| {
        debug_assert_eq!(registered.len(), batch_bytes);
        let started = Instant::now();
        let result = match source {
            TxWindowSource::Bytes(bytes) => {
                let start = usize::try_from(source_offset)
                    .map_err(|_| invalid("TX source offset exceeds usize"))?;
                let end = start
                    .checked_add(registered.len())
                    .ok_or_else(|| invalid("TX source range overflow"))?;
                let source = bytes
                    .get(start..end)
                    .ok_or_else(|| invalid("TX source range exceeds payload"))?;
                registered.copy_from_slice(source);
                Ok(())
            }
            TxWindowSource::Pread(file) => {
                let pread_started = Instant::now();
                let result = read_exact_at(file, source_offset, registered, &mut pread_calls);
                pread_ns = u64::try_from(pread_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                result
            }
        };
        fill_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        result
    })?;

    let batch_bytes_u64 = batch_bytes as u64;
    stats.pread_ns = stats.pread_ns.saturating_add(pread_ns);
    stats.pread_calls = stats.pread_calls.saturating_add(pread_calls);
    if used_pread {
        stats.pread_bytes = stats.pread_bytes.saturating_add(batch_bytes_u64);
    }
    stats.fill_bytes = stats.fill_bytes.saturating_add(batch_bytes_u64);
    stats.fill_ns = stats.fill_ns.saturating_add(fill_ns);
    stats.ring_windows = stats.ring_windows.max(if overlaps_send { 2 } else { 1 });
    if overlaps_send {
        stats.overlap_batches = stats.overlap_batches.saturating_add(1);
    }
    if is_file {
        stats.file_batch_count = stats.file_batch_count.saturating_add(1);
        stats.file_batch_max_bytes = stats.file_batch_max_bytes.max(batch_bytes_u64);
    }
    Ok(PreparedSourceBatch {
        registered,
        lengths,
        bytes: batch_bytes_u64,
    })
}

#[allow(clippy::too_many_arguments)]
fn send_windowed_source(
    source: &mut TxWindowSource<'_>,
    source_length: u64,
    is_file: bool,
    chunk_size: usize,
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    control: &mut TcpStream,
    remote_credit: &mut RemoteReceiveCredit,
    bytes_sent: &mut u64,
    stats: &mut TxFillStats,
) -> Result<u32> {
    if source_length == 0 {
        return Ok(0);
    }
    let batch_capacity = pipeline.configured_window();
    let mut source_offset = 0u64;
    let mut sequence = 0u32;
    let mut prepared = prepare_source_batch(
        source,
        source_length,
        source_offset,
        is_file,
        chunk_size,
        batch_capacity,
        connection,
        stats,
        false,
    )?;

    loop {
        if let Err(error) =
            wait_for_remote_credit_count(control, remote_credit, prepared.lengths.len())
        {
            connection.discard_prepared_batch(prepared.registered)?;
            return Err(error);
        }
        if let Err(error) = drain_pipeline(connection, pipeline) {
            connection.discard_prepared_batch(prepared.registered)?;
            return Err(error);
        }

        let posted =
            connection.post_prepared_batch_tracked(prepared.registered, u64::from(sequence))?;
        debug_assert_eq!(posted, prepared.lengths.len());
        for _ in 0..posted {
            remote_credit.consume()?;
            pipeline.posted()?;
        }
        source_offset = source_offset
            .checked_add(prepared.bytes)
            .ok_or_else(|| Error::Protocol("TX source offset overflow".into()))?;
        *bytes_sent = bytes_sent
            .checked_add(prepared.bytes)
            .ok_or_else(|| Error::Protocol("sent byte count overflow".into()))?;
        sequence = sequence
            .checked_add(
                u32::try_from(posted)
                    .map_err(|_| Error::Protocol("TX batch message count exceeds u32".into()))?,
            )
            .ok_or_else(|| Error::Protocol("URMA sequence overflow".into()))?;

        if source_offset == source_length {
            break;
        }

        let next_count = usize::try_from(
            (source_length - source_offset)
                .div_ceil(chunk_size as u64)
                .min(batch_capacity as u64),
        )
        .map_err(|_| invalid("next TX batch count exceeds usize"))?;
        if connection.tx_slot_state_snapshot().free < next_count {
            drain_pipeline(connection, pipeline)?;
        }
        let overlaps_send = pipeline.current() != 0;
        prepared = prepare_source_batch(
            source,
            source_length,
            source_offset,
            is_file,
            chunk_size,
            batch_capacity,
            connection,
            stats,
            overlaps_send,
        )?;
    }

    Ok(sequence)
}

fn read_exact_at(file: &File, offset: u64, output: &mut [u8], calls: &mut u64) -> Result<()> {
    let mut filled = 0usize;
    while filled < output.len() {
        *calls = calls.saturating_add(1);
        let read = match file.read_at(&mut output[filled..], offset + filled as u64) {
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(io_error("pread benchmark source into registered TX", error));
            }
        };
        if read == 0 {
            return Err(io_error(
                "pread benchmark source into registered TX",
                io::Error::new(io::ErrorKind::UnexpectedEof, "source file ended early"),
            ));
        }
        filled += read;
    }
    Ok(())
}

fn drain_pipeline(
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
) -> Result<()> {
    let mut last_progress = Instant::now();
    while pipeline.current() != 0 {
        let completed = poll_send_completions(connection, pipeline)?;
        if completed != 0 {
            last_progress = Instant::now();
        } else if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
            log_parent_pipeline_timeout(connection, pipeline, "pipeline_drain");
            return Err(Error::Timeout {
                operation: "URMA pipeline drain",
            });
        }
    }
    Ok(())
}

fn poll_send_completions(
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
) -> Result<usize> {
    let mut completed = 0;
    for event in connection.poll_once()? {
        match event {
            CompletionEvent::SendCompleted { .. } => {
                pipeline.completed()?;
                completed += 1;
            }
            CompletionEvent::RecvCompleted { .. } => {
                return Err(Error::Protocol(
                    "unexpected receive CQE while sending URMA payload".into(),
                ))
            }
        }
    }
    debug_assert_eq!(pipeline.current(), connection.outstanding_send());
    Ok(completed)
}

fn replenish_credit(
    connection: &mut UrmaConnection<'_>,
    credit: &mut ReceiveCreditController,
) -> Result<usize> {
    // A completed slot leased to the sink is no longer posted, but it is not
    // physically reusable until the sink returns the whole registered window.
    // Keep the protocol credit accounting separate from physical slot
    // availability and only refill slots that are actually Free.
    let free_rx_slots = connection.rx_slot_state_snapshot().free;
    let repost_count = bounded_repost_count(credit.posts_needed(), free_rx_slots);
    let first_sequence = credit.next_post_sequence();
    let posted = connection.recv_ready_tracked_batch(first_sequence, repost_count)?;
    for _ in 0..posted {
        credit.posted()?;
    }
    Ok(posted)
}

fn log_parent_pipeline_timeout(
    connection: &UrmaConnection<'_>,
    pipeline: &PipelineTracker,
    operation: &str,
) {
    let stats = connection.stats();
    let slots = connection.tx_slot_state_snapshot();
    let diagnostic = connection.pending_send_diagnostic();
    eprintln!(
        "{{\"event\":\"urma_benchmark_timeout\",\"role\":\"parent\",\"operation\":\"{}\",\"configured_window\":{},\"current_outstanding_send\":{},\"pipeline_tracker_current\":{},\"max_outstanding_send\":{},\"send_post\":{},\"send_cqe\":{},\"recv_post\":{},\"recv_cqe\":{},\"cqe_error\":{},\"poll_calls\":{},\"empty_polls\":{},\"connection_outstanding_send\":{},\"last_completed_sequence\":{},\"pending_send\":{},\"tx_slots\":{{\"free\":{},\"allocated\":{},\"send_posted\":{},\"send_completed\":{},\"other\":{}}}}}",
        operation,
        pipeline.configured_window(),
        connection.outstanding_send(),
        pipeline.current(),
        stats.max_outstanding_send,
        stats.send_post,
        stats.send_cqe,
        stats.recv_post,
        stats.recv_cqe,
        stats.cqe_error,
        stats.poll_calls,
        stats.empty_polls,
        connection.outstanding_send(),
        optional_sequence_json(diagnostic.last_completed_sequence),
        pending_wr_json(&diagnostic.pending),
        slots.free,
        slots.allocated,
        slots.send_posted,
        slots.send_completed,
        slots.other,
    );
}

fn log_child_receive_timeout(connection: &UrmaConnection<'_>, credit: &ReceiveCreditController) {
    let stats = connection.stats();
    let slots = connection.rx_slot_state_snapshot();
    let diagnostic = connection.pending_recv_diagnostic();
    eprintln!(
        "{{\"event\":\"urma_benchmark_timeout\",\"role\":\"child\",\"operation\":\"benchmark_receive\",\"configured_receive_credit\":{},\"current_receive_credit\":{},\"benchmark_credit_current\":{},\"benchmark_credit_remaining_messages\":{},\"recv_post\":{},\"recv_cqe\":{},\"send_post\":{},\"send_cqe\":{},\"cqe_error\":{},\"poll_calls\":{},\"empty_polls\":{},\"connection_outstanding_recv\":{},\"last_completed_sequence\":{},\"pending_recv\":{},\"rx_slots\":{{\"free\":{},\"allocated\":{},\"posted_recv\":{},\"recv_completed\":{},\"other\":{}}}}}",
        credit.configured_credit(),
        connection.receive_credit(),
        credit.current_credit(),
        credit.remaining_messages(),
        stats.recv_post,
        stats.recv_cqe,
        stats.send_post,
        stats.send_cqe,
        stats.cqe_error,
        stats.poll_calls,
        stats.empty_polls,
        connection.outstanding_recv(),
        optional_sequence_json(diagnostic.last_completed_sequence),
        pending_wr_json(&diagnostic.pending),
        slots.free,
        slots.allocated,
        slots.posted_recv,
        slots.recv_completed,
        slots.other,
    );
}

fn optional_sequence_json(sequence: Option<u64>) -> String {
    sequence.map_or_else(|| "null".into(), |value| value.to_string())
}

fn pending_wr_json(pending: &[crate::PendingWrSnapshot]) -> String {
    let entries = pending
        .iter()
        .map(|item| {
            format!(
                "{{\"sequence\":{},\"slot_id\":{},\"slot_state\":\"{:?}\"}}",
                optional_sequence_json(item.sequence),
                item.slot.index(),
                item.state,
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", entries.join(","))
}

fn setup_measurement(mode: TimingMode) -> Result<Option<Measurement>> {
    if mode == TimingMode::SetupIncluded {
        Ok(Some(Measurement::start(mode)?))
    } else {
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug)]
struct CpuSnapshot(CpuUsage);

impl CpuSnapshot {
    fn capture() -> Result<Self> {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
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

fn timeval_us(value: libc::timeval) -> Result<u64> {
    let seconds = u64::try_from(value.tv_sec).map_err(|_| invalid("negative CPU seconds"))?;
    let micros = u64::try_from(value.tv_usec).map_err(|_| invalid("negative CPU micros"))?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
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
        Ok((
            self.timer.finish(),
            CpuSnapshot::capture()?.elapsed_since(self.cpu)?,
        ))
    }
}

fn combined_stats(
    case: &BenchmarkCase,
    runtime: &RuntimeConfig,
    parent: CompletionStats,
    child: CompletionStats,
    bytes_sent: u64,
    bytes_received: u64,
    configured_receive_credit: usize,
) -> Result<UrmaTransportStats> {
    let total_registered_bytes = runtime.buffer_pool.total_len()?;
    let poll_calls = parent.poll_calls.saturating_add(child.poll_calls);
    let empty_polls = parent.empty_polls.saturating_add(child.empty_polls);
    let nonempty_polls = parent.nonempty_polls.saturating_add(child.nonempty_polls);
    let completion_batch_total = parent
        .completion_batch_total
        .saturating_add(child.completion_batch_total);
    Ok(UrmaTransportStats {
        send_post: parent.send_post,
        recv_post: child.recv_post,
        send_post_calls: parent.send_post_calls,
        recv_post_calls: child.recv_post_calls,
        send_post_list_max: parent.send_post_list_max,
        recv_post_list_max: child.recv_post_list_max,
        send_cqe: parent.send_cqe,
        send_retired: parent.send_retired,
        recv_cqe: child.recv_cqe,
        cqe_error: parent.cqe_error + child.cqe_error,
        poll_calls,
        empty_polls,
        send_jfc_poll_calls: parent
            .send_jfc_poll_calls
            .saturating_add(child.send_jfc_poll_calls),
        send_jfc_empty_polls: parent
            .send_jfc_empty_polls
            .saturating_add(child.send_jfc_empty_polls),
        recv_jfc_poll_calls: parent
            .recv_jfc_poll_calls
            .saturating_add(child.recv_jfc_poll_calls),
        recv_jfc_empty_polls: parent
            .recv_jfc_empty_polls
            .saturating_add(child.recv_jfc_empty_polls),
        yield_count: parent.yield_count.saturating_add(child.yield_count),
        sleep_count: parent.sleep_count.saturating_add(child.sleep_count),
        backoff_sleep_ns: parent
            .backoff_sleep_ns
            .saturating_add(child.backoff_sleep_ns),
        jfc_rearm_count: parent.jfc_rearm_count.saturating_add(child.jfc_rearm_count),
        event_wait_count: parent
            .event_wait_count
            .saturating_add(child.event_wait_count),
        event_wakeup_count: parent
            .event_wakeup_count
            .saturating_add(child.event_wakeup_count),
        event_timeout_count: parent
            .event_timeout_count
            .saturating_add(child.event_timeout_count),
        spurious_wakeup_count: parent
            .spurious_wakeup_count
            .saturating_add(child.spurious_wakeup_count),
        event_wait_ns: parent.event_wait_ns.saturating_add(child.event_wait_ns),
        max_event_wait_ns: parent.max_event_wait_ns.max(child.max_event_wait_ns),
        max_empty_streak: parent.max_empty_streak.max(child.max_empty_streak),
        nonempty_polls,
        completion_batch_total,
        avg_poll_batch_milli: scaled_ratio(completion_batch_total, nonempty_polls, 1_000),
        empty_poll_ratio_ppm: scaled_ratio(empty_polls, poll_calls, 1_000_000),
        max_completion_poll_gap_ns: parent
            .max_completion_poll_gap_ns
            .max(child.max_completion_poll_gap_ns),
        max_outstanding_send: parent.max_outstanding_send,
        current_outstanding_send: 0,
        current_outstanding_recv: 0,
        configured_window: u64::from(case.window),
        configured_receive_credit: u64::try_from(configured_receive_credit)
            .map_err(|_| invalid("configured receive credit does not fit u64"))?,
        slot_size: u64::try_from(runtime.buffer_pool.slot_size)
            .map_err(|_| invalid("slot_size does not fit result u64"))?,
        effective_payload_size: case.chunk_size,
        tx_slot_count: u64::try_from(runtime.buffer_pool.tx_slot_count)
            .map_err(|_| invalid("TX slot count does not fit result u64"))?,
        rx_slot_count: u64::try_from(runtime.buffer_pool.rx_slot_count)
            .map_err(|_| invalid("RX slot count does not fit result u64"))?,
        total_registered_bytes: u64::try_from(total_registered_bytes)
            .map_err(|_| invalid("registered pool size does not fit result u64"))?,
        bytes_sent,
        bytes_received,
    })
}

fn benchmark_runtime_config(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    profile: UrmaBenchmarkProfile,
    child: bool,
) -> Result<RuntimeConfig> {
    let mut config = RuntimeConfig::new(device, eid_index);
    config.buffer_pool.slot_size = derive_urma_slot_size(case, config.buffer_pool.alignment)?;
    config.buffer_pool.alias_tx_slots = profile.uses_fixed_tx();
    config.buffer_pool.rx_slot_count = profile.rx_slots(case, child)?;
    config.buffer_pool.total_len()?;
    Ok(config)
}

fn expect_case_control(session: &mut OobSession, kind: u16, case_id: &str) -> Result<()> {
    let payload = read_control(session.stream_mut(), kind)?;
    if payload != case_id.as_bytes() {
        return Err(Error::Protocol(
            "URMA benchmark control case_id mismatch".into(),
        ));
    }
    Ok(())
}

fn configure_control_stream(stream: &TcpStream) -> Result<()> {
    stream
        .set_read_timeout(Some(TIMEOUT))
        .and_then(|_| stream.set_write_timeout(Some(TIMEOUT)))
        .map_err(|error| io_error("configure URMA benchmark control timeout", error))
}

fn encode_ready(
    case_id: &str,
    initial_credit: usize,
    profile: UrmaBenchmarkProfile,
) -> Result<Vec<u8>> {
    let case = case_id.as_bytes();
    let case_len = u16::try_from(case.len()).map_err(|_| invalid("case_id too long"))?;
    let credit = u32::try_from(initial_credit)
        .map_err(|_| invalid("initial remote receive credit exceeds u32"))?;
    let mut payload = Vec::with_capacity(2 + case.len() + 4 + 1);
    payload.extend_from_slice(&case_len.to_be_bytes());
    payload.extend_from_slice(case);
    payload.extend_from_slice(&credit.to_be_bytes());
    payload.push(profile.wire_id());
    Ok(payload)
}

fn decode_ready(payload: &[u8]) -> Result<(String, usize, u8)> {
    if payload.len() < 7 {
        return Err(Error::Protocol("truncated URMA READY payload".into()));
    }
    let case_len = u16::from_be_bytes(payload[..2].try_into().expect("fixed slice")) as usize;
    let expected_len = 2usize
        .checked_add(case_len)
        .and_then(|length| length.checked_add(5))
        .ok_or_else(|| Error::Protocol("URMA READY length overflow".into()))?;
    if payload.len() != expected_len {
        return Err(Error::Protocol("invalid URMA READY payload length".into()));
    }
    let case_id = std::str::from_utf8(&payload[2..2 + case_len])
        .map_err(|_| Error::Protocol("URMA READY case_id is not UTF-8".into()))?
        .to_owned();
    let credit = u32::from_be_bytes(
        payload[2 + case_len..expected_len - 1]
            .try_into()
            .expect("fixed slice"),
    ) as usize;
    if credit == 0 {
        return Err(Error::Protocol(
            "URMA READY advertised zero receive credit".into(),
        ));
    }
    Ok((case_id, credit, payload[expected_len - 1]))
}

fn expect_ready(
    session: &mut OobSession,
    case_id: &str,
    expected_credit: usize,
    profile: UrmaBenchmarkProfile,
) -> Result<usize> {
    let (received_case_id, credit, received_profile) =
        decode_ready(&read_control(session.stream_mut(), READY)?)?;
    if received_case_id != case_id
        || credit != expected_credit
        || received_profile != profile.wire_id()
    {
        return Err(Error::Protocol(format!(
            "URMA READY mismatch: case_id={received_case_id:?}, credit={credit}, profile={received_profile}, expected case_id={case_id:?}, credit={expected_credit}, profile={}",
            profile.wire_id()
        )));
    }
    Ok(credit)
}

fn write_credit(stream: &mut TcpStream, count: usize) -> Result<()> {
    let count = u32::try_from(count).map_err(|_| invalid("remote credit update exceeds u32"))?;
    if count == 0 {
        return Err(invalid("remote credit update must be non-zero"));
    }
    write_control(stream, CREDIT, &count.to_be_bytes())
}

fn decode_credit(payload: &[u8]) -> Result<usize> {
    if payload.len() != 4 {
        return Err(Error::Protocol("invalid URMA CREDIT payload length".into()));
    }
    let count = u32::from_be_bytes(payload.try_into().expect("fixed slice")) as usize;
    if count == 0 {
        return Err(Error::Protocol("zero URMA CREDIT update".into()));
    }
    Ok(count)
}

fn wait_for_remote_credit(
    stream: &mut TcpStream,
    remote_credit: &mut RemoteReceiveCredit,
) -> Result<()> {
    wait_for_remote_credit_count(stream, remote_credit, 1)
}

fn wait_for_remote_credit_count(
    stream: &mut TcpStream,
    remote_credit: &mut RemoteReceiveCredit,
    required: usize,
) -> Result<()> {
    if required == 0 || required > remote_credit.initial() {
        return Err(invalid(
            "remote credit batch must be within the initial RQ capacity",
        ));
    }
    if remote_credit.available() >= required {
        return Ok(());
    }
    remote_credit.waited()?;
    let wait_started = Instant::now();
    while remote_credit.available() < required {
        let (kind, payload) = read_control_frame(stream)?;
        if kind != CREDIT {
            return Err(Error::Protocol(format!(
                "received control kind {kind} while waiting for remote RX credit"
            )));
        }
        remote_credit.grant(decode_credit(&payload)?)?;
    }
    remote_credit.record_wait_duration(wait_started.elapsed());
    Ok(())
}

fn read_done(stream: &mut TcpStream, remote_credit: &mut RemoteReceiveCredit) -> Result<Vec<u8>> {
    loop {
        let (kind, payload) = read_control_frame(stream)?;
        match kind {
            CREDIT => remote_credit.grant(decode_credit(&payload)?)?,
            DONE => return Ok(payload),
            _ => {
                return Err(Error::Protocol(format!(
                    "received control kind {kind} while waiting for DONE"
                )))
            }
        }
    }
}

fn insert_remote_credit_stats(result: &mut BenchmarkResult, credit: &RemoteReceiveCredit) {
    for (name, value) in [
        ("remote_credit_initial", credit.initial()),
        ("remote_credit_returned", credit.returned()),
        ("remote_credit_consumed", credit.consumed()),
        ("remote_credit_updates", credit.updates()),
        ("remote_credit_wait_count", credit.waits()),
        ("remote_credit_pending", credit.available()),
    ] {
        result
            .transport_stats
            .insert(name.into(), u64::try_from(value).unwrap_or(u64::MAX));
    }
    result
        .transport_stats
        .insert("remote_credit_wait_ns".into(), credit.wait_ns());
    result
        .transport_stats
        .insert("remote_credit_max_wait_ns".into(), credit.max_wait_ns());
}

fn write_control(stream: &mut TcpStream, kind: u16, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_CONTROL_PAYLOAD {
        return Err(invalid("URMA benchmark control payload too large"));
    }
    let mut header = Vec::with_capacity(CONTROL_HEADER_LEN);
    header.extend_from_slice(&CONTROL_MAGIC.to_be_bytes());
    header.extend_from_slice(&CONTROL_VERSION.to_be_bytes());
    header.extend_from_slice(&kind.to_be_bytes());
    header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(payload))
        .map_err(|error| io_error("write URMA benchmark control", error))
}

fn read_control(stream: &mut TcpStream, expected_kind: u16) -> Result<Vec<u8>> {
    let (kind, payload) = read_control_frame(stream)?;
    if kind != expected_kind {
        return Err(Error::Protocol(format!(
            "received URMA control kind {kind}, expected {expected_kind}"
        )));
    }
    Ok(payload)
}

fn read_control_frame(stream: &mut TcpStream) -> Result<(u16, Vec<u8>)> {
    let mut header = [0u8; CONTROL_HEADER_LEN];
    stream
        .read_exact(&mut header)
        .map_err(|error| io_error("read URMA benchmark control header", error))?;
    let magic = u32::from_be_bytes(header[0..4].try_into().expect("fixed slice"));
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed slice"));
    let kind = u16::from_be_bytes(header[6..8].try_into().expect("fixed slice"));
    let length = u32::from_be_bytes(header[8..12].try_into().expect("fixed slice")) as usize;
    if magic != CONTROL_MAGIC || version != CONTROL_VERSION || length > MAX_CONTROL_PAYLOAD {
        return Err(Error::Protocol(
            "invalid URMA benchmark control frame".into(),
        ));
    }
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .map_err(|error| io_error("read URMA benchmark control payload", error))?;
    Ok((kind, payload))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Done {
    case_id: String,
    integrity: IntegrityResult,
    elapsed_ns: u64,
    child_cpu: CpuUsage,
    completion: CompletionStats,
    payload_poll: PayloadPollStats,
    bytes_received: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PayloadPollStats {
    poll_calls: u64,
    empty_polls: u64,
    send_jfc_poll_calls: u64,
    send_jfc_empty_polls: u64,
    recv_jfc_poll_calls: u64,
    recv_jfc_empty_polls: u64,
}

impl PayloadPollStats {
    fn from_completion(stats: CompletionStats) -> Self {
        Self {
            poll_calls: stats.poll_calls,
            empty_polls: stats.empty_polls,
            send_jfc_poll_calls: stats.send_jfc_poll_calls,
            send_jfc_empty_polls: stats.send_jfc_empty_polls,
            recv_jfc_poll_calls: stats.recv_jfc_poll_calls,
            recv_jfc_empty_polls: stats.recv_jfc_empty_polls,
        }
    }

    fn elapsed_since(self, start: Self) -> Self {
        Self {
            poll_calls: self.poll_calls.saturating_sub(start.poll_calls),
            empty_polls: self.empty_polls.saturating_sub(start.empty_polls),
            send_jfc_poll_calls: self
                .send_jfc_poll_calls
                .saturating_sub(start.send_jfc_poll_calls),
            send_jfc_empty_polls: self
                .send_jfc_empty_polls
                .saturating_sub(start.send_jfc_empty_polls),
            recv_jfc_poll_calls: self
                .recv_jfc_poll_calls
                .saturating_sub(start.recv_jfc_poll_calls),
            recv_jfc_empty_polls: self
                .recv_jfc_empty_polls
                .saturating_sub(start.recv_jfc_empty_polls),
        }
    }

    fn insert(self, result: &mut BenchmarkResult, role: &str) {
        for (suffix, value) in [
            ("poll_calls", self.poll_calls),
            ("empty_polls", self.empty_polls),
            ("send_jfc_poll_calls", self.send_jfc_poll_calls),
            ("send_jfc_empty_polls", self.send_jfc_empty_polls),
            ("recv_jfc_poll_calls", self.recv_jfc_poll_calls),
            ("recv_jfc_empty_polls", self.recv_jfc_empty_polls),
        ] {
            result
                .transport_stats
                .insert(format!("payload_{role}_{suffix}"), value);
        }
        result.transport_stats.insert(
            format!("payload_{role}_empty_poll_ratio_ppm"),
            scaled_ratio(self.empty_polls, self.poll_calls, 1_000_000),
        );
        result.transport_stats.insert(
            format!("payload_{role}_send_jfc_empty_ratio_ppm"),
            scaled_ratio(
                self.send_jfc_empty_polls,
                self.send_jfc_poll_calls,
                1_000_000,
            ),
        );
        result.transport_stats.insert(
            format!("payload_{role}_recv_jfc_empty_ratio_ppm"),
            scaled_ratio(
                self.recv_jfc_empty_polls,
                self.recv_jfc_poll_calls,
                1_000_000,
            ),
        );
    }
}

fn encode_done(done: &Done) -> Result<Vec<u8>> {
    let case = done.case_id.as_bytes();
    let case_len = u16::try_from(case.len()).map_err(|_| invalid("case_id too long"))?;
    let mut output = Vec::with_capacity(128 + case.len());
    output.extend_from_slice(&case_len.to_be_bytes());
    output.extend_from_slice(case);
    for value in [
        done.integrity.expected_bytes,
        done.integrity.actual_bytes,
        done.elapsed_ns,
        done.child_cpu.user_us,
        done.child_cpu.system_us,
        done.completion.send_post,
        done.completion.recv_post,
        done.completion.send_post_calls,
        done.completion.recv_post_calls,
        done.completion.send_post_list_max,
        done.completion.recv_post_list_max,
        done.completion.send_cqe,
        done.completion.send_retired,
        done.completion.recv_cqe,
        done.completion.cqe_error,
        done.completion.poll_calls,
        done.completion.empty_polls,
        done.completion.send_jfc_poll_calls,
        done.completion.send_jfc_empty_polls,
        done.completion.recv_jfc_poll_calls,
        done.completion.recv_jfc_empty_polls,
        done.completion.yield_count,
        done.completion.sleep_count,
        done.completion.backoff_sleep_ns,
        done.completion.jfc_rearm_count,
        done.completion.event_wait_count,
        done.completion.event_wakeup_count,
        done.completion.event_timeout_count,
        done.completion.spurious_wakeup_count,
        done.completion.event_wait_ns,
        done.completion.max_event_wait_ns,
        done.completion.max_empty_streak,
        done.completion.nonempty_polls,
        done.completion.completion_batch_total,
        done.completion.max_completion_poll_gap_ns,
        done.completion.max_outstanding_send,
        done.payload_poll.poll_calls,
        done.payload_poll.empty_polls,
        done.payload_poll.send_jfc_poll_calls,
        done.payload_poll.send_jfc_empty_polls,
        done.payload_poll.recv_jfc_poll_calls,
        done.payload_poll.recv_jfc_empty_polls,
        done.bytes_received,
    ] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    output.extend_from_slice(&done.integrity.expected_crc32.to_be_bytes());
    output.extend_from_slice(&done.integrity.actual_crc32.to_be_bytes());
    Ok(output)
}

fn decode_done(input: &[u8]) -> Result<Done> {
    if input.len() < 2 {
        return Err(Error::Protocol("truncated URMA Done".into()));
    }
    let case_len = u16::from_be_bytes([input[0], input[1]]) as usize;
    let expected_len = 2 + case_len + 43 * 8 + 2 * 4;
    if input.len() != expected_len {
        return Err(Error::Protocol("invalid URMA Done length".into()));
    }
    let case_id = std::str::from_utf8(&input[2..2 + case_len])
        .map_err(|_| Error::Protocol("URMA Done case_id is not UTF-8".into()))?
        .to_owned();
    let mut offset = 2 + case_len;
    let mut next_u64 = || {
        let value = u64::from_be_bytes(input[offset..offset + 8].try_into().expect("fixed slice"));
        offset += 8;
        value
    };
    let expected_bytes = next_u64();
    let actual_bytes = next_u64();
    let elapsed_ns = next_u64();
    let user_us = next_u64();
    let system_us = next_u64();
    let send_post = next_u64();
    let recv_post = next_u64();
    let send_post_calls = next_u64();
    let recv_post_calls = next_u64();
    let send_post_list_max = next_u64();
    let recv_post_list_max = next_u64();
    let send_cqe = next_u64();
    let send_retired = next_u64();
    let recv_cqe = next_u64();
    let cqe_error = next_u64();
    let poll_calls = next_u64();
    let empty_polls = next_u64();
    let send_jfc_poll_calls = next_u64();
    let send_jfc_empty_polls = next_u64();
    let recv_jfc_poll_calls = next_u64();
    let recv_jfc_empty_polls = next_u64();
    let yield_count = next_u64();
    let sleep_count = next_u64();
    let backoff_sleep_ns = next_u64();
    let jfc_rearm_count = next_u64();
    let event_wait_count = next_u64();
    let event_wakeup_count = next_u64();
    let event_timeout_count = next_u64();
    let spurious_wakeup_count = next_u64();
    let event_wait_ns = next_u64();
    let max_event_wait_ns = next_u64();
    let max_empty_streak = next_u64();
    let nonempty_polls = next_u64();
    let completion_batch_total = next_u64();
    let max_completion_poll_gap_ns = next_u64();
    let max_outstanding_send = next_u64();
    let payload_poll = PayloadPollStats {
        poll_calls: next_u64(),
        empty_polls: next_u64(),
        send_jfc_poll_calls: next_u64(),
        send_jfc_empty_polls: next_u64(),
        recv_jfc_poll_calls: next_u64(),
        recv_jfc_empty_polls: next_u64(),
    };
    let bytes_received = next_u64();
    let expected_crc32 = u32::from_be_bytes(input[offset..offset + 4].try_into().expect("fixed"));
    offset += 4;
    let actual_crc32 = u32::from_be_bytes(input[offset..offset + 4].try_into().expect("fixed"));
    Ok(Done {
        case_id,
        integrity: IntegrityResult::new(expected_bytes, actual_bytes, expected_crc32, actual_crc32),
        elapsed_ns,
        child_cpu: CpuUsage { user_us, system_us },
        completion: CompletionStats {
            send_post,
            recv_post,
            send_post_calls,
            recv_post_calls,
            send_post_list_max,
            recv_post_list_max,
            send_cqe,
            send_retired,
            recv_cqe,
            cqe_error,
            poll_calls,
            empty_polls,
            send_jfc_poll_calls,
            send_jfc_empty_polls,
            recv_jfc_poll_calls,
            recv_jfc_empty_polls,
            yield_count,
            sleep_count,
            backoff_sleep_ns,
            jfc_rearm_count,
            event_wait_count,
            event_wakeup_count,
            event_timeout_count,
            spurious_wakeup_count,
            event_wait_ns,
            max_event_wait_ns,
            max_empty_streak,
            nonempty_polls,
            completion_batch_total,
            max_completion_poll_gap_ns,
            max_outstanding_send,
        },
        payload_poll,
        bytes_received,
    })
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

    fn memory_case(bytes: u64, window: u32) -> BenchmarkCase {
        BenchmarkCase::new(
            "native-test",
            1,
            BenchmarkScenario::Memory,
            crate::BenchmarkTransport::Urma,
            bytes,
            64 * 1024,
            window,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            0,
        )
        .unwrap()
    }

    #[test]
    fn performance_profiles_size_rx_backing_independently_of_jfr_depth() {
        let case = memory_case(2 * 1024 * 1024 * 1024, 128);
        assert_eq!(
            UrmaBenchmarkProfile::Normal.rx_slots(&case, false).unwrap(),
            512
        );
        assert_eq!(
            UrmaBenchmarkProfile::Normal.rx_slots(&case, true).unwrap(),
            4096
        );
        assert_eq!(
            UrmaBenchmarkProfile::TransportOnly
                .rx_slots(&case, true)
                .unwrap(),
            32_769
        );
        assert_eq!(
            UrmaBenchmarkProfile::FixedTxTransportOnly
                .rx_slots(&case, true)
                .unwrap(),
            32_769
        );
        assert!(UrmaBenchmarkProfile::FixedTxTransportOnly.uses_fixed_tx());
        assert!(UrmaBenchmarkProfile::FixedTxTransportOnly.transport_only());
    }

    #[test]
    fn crc_worker_selection_respects_affinity_and_explicit_limit() {
        assert_eq!(select_crc_worker_count(256, None, 25).unwrap(), 24);
        assert_eq!(select_crc_worker_count(256, Some(24), 25).unwrap(), 24);
        assert_eq!(select_crc_worker_count(16, None, 64).unwrap(), 16);
        assert!(select_crc_worker_count(256, Some(25), 25).is_err());
        assert!(select_crc_worker_count(256, Some(33), 64).is_err());
        assert!(select_crc_worker_count(256, Some(0), 64).is_err());
    }

    #[test]
    fn fixed_memory_source_is_virtual_and_matches_case_length() {
        let case = memory_case(8 * 1024 * 1024 * 1024, 64);
        let source = UrmaBenchmarkSource::fixed_memory(case.transfer_bytes);
        assert!(source.validate(&case).is_ok());
        assert!(source.expected_crc32().is_err());
    }

    #[test]
    fn repeated_payload_crc_combine_matches_linear_hashing() {
        let payload = b"fixed-payload";
        for repetitions in [0, 1, 2, 3, 7, 32, 255, 1024] {
            let mut linear = Crc32Hasher::new();
            for _ in 0..repetitions {
                linear.update(payload);
            }
            assert_eq!(
                repeated_payload_crc32(payload, repetitions),
                linear.finalize(),
                "repetitions={repetitions}"
            );
        }
    }

    #[test]
    fn registered_windows_partition_the_physical_rq() {
        assert_eq!(registered_rx_window_chunks(128, 512).unwrap(), 128);
        assert_eq!(registered_rx_window_chunks(100, 512).unwrap(), 64);
        assert_eq!(registered_rx_window_chunks(128, 128).unwrap(), 128);
        assert_eq!(registered_rx_window_chunks(3, 8).unwrap(), 2);
    }

    #[test]
    fn receive_refill_is_bounded_by_physically_free_slots() {
        assert_eq!(bounded_repost_count(16, 0), 0);
        assert_eq!(bounded_repost_count(128, 32), 32);
        assert_eq!(bounded_repost_count(64, 512), 64);
    }

    #[test]
    fn registered_rx_pipeline_preserves_chunk_order_and_integrity() {
        let chunks: [&[u8]; 3] = [b"abcd", b"efgh", b"ijk"];
        let payload = chunks.concat();
        let expected_crc32 = crate::crc32_bytes(&payload);
        let sink = WindowSink::memory(payload.len() as u64, expected_crc32);
        let mut pipeline = SinkPipeline::start(sink, 2, false).unwrap();
        pipeline
            .push(
                0,
                RegisteredRxWindowLease::from_test_bytes(
                    chunks[0].to_vec(),
                    vec![(Some(0), chunks[0].len())],
                ),
            )
            .unwrap();
        pipeline
            .push(
                chunks[0].len() as u64,
                RegisteredRxWindowLease::from_test_parts(
                    vec![chunks[1].to_vec(), chunks[2].to_vec()],
                    vec![(Some(1), chunks[1].len()), (Some(2), chunks[2].len())],
                ),
            )
            .unwrap();
        let (integrity, recycled) = pipeline.finish().unwrap();
        assert!(integrity.is_ok());
        assert_eq!(recycled.len(), 2);
    }

    #[test]
    fn out_of_order_worker_results_retire_leases_in_wire_order() {
        let sink = WindowSink::memory(2, crate::crc32_bytes(b"ab"));
        let mut pipeline = SinkPipeline::start(sink, 2, true).unwrap();

        let outcome = |order, sequence, byte| {
            let mut digest = Crc32Hasher::new();
            digest.update(&[byte]);
            WindowOutcome {
                order,
                length: 1,
                digest: Ok(digest),
                window: RegisteredRxWindowLease::from_test_bytes(
                    vec![byte],
                    vec![(Some(sequence), 1)],
                ),
            }
        };

        pipeline.accept_outcome(outcome(1, 1, b'b'));
        assert!(pipeline.recycled.is_empty());
        pipeline.accept_outcome(outcome(0, 0, b'a'));

        let retired_sequences = pipeline
            .recycled
            .iter()
            .map(|window| window.chunks()[0].sequence.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(retired_sequences, vec![0, 1]);
    }

    #[test]
    fn transport_only_defers_crc_until_finish() {
        let payload = b"deferred-transport-verification".to_vec();
        let sink = WindowSink::memory(payload.len() as u64, crate::crc32_bytes(&payload));
        let mut pipeline = SinkPipeline::start(sink, 2, true).unwrap();
        pipeline
            .push(
                0,
                RegisteredRxWindowLease::from_test_bytes(
                    payload.clone(),
                    vec![(Some(0), payload.len())],
                ),
            )
            .unwrap();
        assert!(pipeline.take_recycled().unwrap().is_empty());
        let (integrity, recycled) = pipeline.finish().unwrap();
        assert!(integrity.is_ok());
        assert_eq!(recycled.len(), 1);
    }

    #[test]
    fn registered_rx_pipeline_reports_sequence_failure() {
        let payload = b"abcdefgh".to_vec();
        let expected_crc32 = crate::crc32_bytes(&payload);
        let metadata = IntegrationMessageV3::metadata(
            REQUEST_ID,
            0,
            payload.len() as u64,
            DigestDescriptor::crc32(expected_crc32),
        );
        let mut receiver =
            UrmaReceiveState::new(REQUEST_ID, payload.len() as u64, expected_crc32).unwrap();
        receiver.accept_metadata(&metadata).unwrap();
        receiver.accept_data_length(0, 4).unwrap();
        let error = receiver.accept_data_length(0, 4).unwrap_err();
        assert!(matches!(error, Error::Protocol(_)));
    }

    #[test]
    fn direct_file_sink_pwrites_and_hashes_each_window() {
        let payload = b"registered-window-direct-file";
        let expected_crc32 = crate::crc32_bytes(payload);
        let path = std::env::temp_dir().join(format!(
            "urma-direct-sink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sink = WindowSink::file(
            &path,
            payload.len() as u64,
            expected_crc32,
            FileCompletionPolicy::Buffered,
        )
        .unwrap();
        let mut pipeline = SinkPipeline::start(sink, 2, false).unwrap();
        pipeline
            .push(
                0,
                RegisteredRxWindowLease::from_test_bytes(
                    payload[..10].to_vec(),
                    vec![(Some(0), 10)],
                ),
            )
            .unwrap();
        pipeline
            .push(
                10,
                RegisteredRxWindowLease::from_test_bytes(
                    payload[10..].to_vec(),
                    vec![(Some(1), payload.len() - 10)],
                ),
            )
            .unwrap();
        assert!(pipeline.finish().unwrap().0.is_ok());
        assert_eq!(std::fs::read(&path).unwrap(), payload);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn positional_source_read_fills_exact_registered_range() {
        let path = std::env::temp_dir().join(format!(
            "urma-direct-source-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"0123456789").unwrap();
        let file = File::open(&path).unwrap();
        let mut output = [0u8; 5];
        let mut calls = 0;
        read_exact_at(&file, 3, &mut output, &mut calls).unwrap();
        assert_eq!(&output, b"34567");
        assert_eq!(calls, 1);

        let mut too_long = [0u8; 4];
        assert!(read_exact_at(&file, 8, &mut too_long, &mut calls).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn tx_batches_keep_wr_chunks_within_provider_payload() {
        let chunk = 64 * 1024;
        let full = tx_batch_lengths((chunk * 128) as u64, chunk, 64).unwrap();
        assert_eq!(full.len(), 64);
        assert!(full.iter().all(|&length| length == chunk));
        assert_eq!(full.iter().sum::<usize>(), 4 * 1024 * 1024);

        let tail = tx_batch_lengths((chunk * 2 + 17) as u64, chunk, 64).unwrap();
        assert_eq!(tail, vec![chunk, chunk, 17]);
        assert!(tx_batch_lengths(0, chunk, 64).is_err());
        assert!(tx_batch_lengths(1, 0, 64).is_err());
        assert!(tx_batch_lengths(1, chunk, 0).is_err());
    }

    #[test]
    fn mmap_source_exposes_finished_file_without_changing_it() {
        let path = std::env::temp_dir().join(format!(
            "urma-mmap-source-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let expected = b"finished-piece-source";
        std::fs::write(&path, expected).unwrap();
        let source = FileSource::from_path(&path).unwrap();
        {
            let mapped = MappedFileSource::map(&source).unwrap().unwrap();
            assert_eq!(mapped.as_slice(), expected);
        }
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ready_control_round_trip_binds_case_and_posted_credit() {
        let profile = UrmaBenchmarkProfile::FixedTxTransportOnly;
        let payload = encode_ready("credit-case", 512, profile).unwrap();
        assert_eq!(
            decode_ready(&payload).unwrap(),
            ("credit-case".to_string(), 512, profile.wire_id())
        );

        let mut truncated = payload.clone();
        truncated.pop();
        assert!(decode_ready(&truncated).is_err());

        let zero = encode_ready("credit-case", 0, profile).unwrap();
        assert!(decode_ready(&zero).is_err());
    }

    #[test]
    fn credit_control_rejects_zero_and_wrong_length() {
        assert_eq!(decode_credit(&128u32.to_be_bytes()).unwrap(), 128);
        assert!(decode_credit(&0u32.to_be_bytes()).is_err());
        assert!(decode_credit(&[0, 1]).is_err());
    }

    #[test]
    fn done_control_round_trip_preserves_hybrid_polling_stats() {
        let done = Done {
            case_id: "hybrid-poll".into(),
            integrity: IntegrityResult::new(64, 64, 7, 7),
            elapsed_ns: 11,
            child_cpu: CpuUsage {
                user_us: 13,
                system_us: 17,
            },
            completion: CompletionStats {
                send_post: 1,
                recv_post: 2,
                send_post_calls: 1,
                recv_post_calls: 1,
                send_post_list_max: 1,
                recv_post_list_max: 2,
                send_cqe: 3,
                send_retired: 4,
                recv_cqe: 5,
                cqe_error: 6,
                poll_calls: 7,
                empty_polls: 8,
                send_jfc_poll_calls: 9,
                send_jfc_empty_polls: 10,
                recv_jfc_poll_calls: 11,
                recv_jfc_empty_polls: 12,
                yield_count: 13,
                sleep_count: 14,
                backoff_sleep_ns: 15,
                jfc_rearm_count: 16,
                event_wait_count: 17,
                event_wakeup_count: 18,
                event_timeout_count: 19,
                spurious_wakeup_count: 20,
                event_wait_ns: 21,
                max_event_wait_ns: 22,
                max_empty_streak: 23,
                nonempty_polls: 24,
                completion_batch_total: 25,
                max_completion_poll_gap_ns: 26,
                max_outstanding_send: 27,
            },
            payload_poll: PayloadPollStats {
                poll_calls: 28,
                empty_polls: 29,
                send_jfc_poll_calls: 30,
                send_jfc_empty_polls: 31,
                recv_jfc_poll_calls: 32,
                recv_jfc_empty_polls: 33,
            },
            bytes_received: 64,
        };

        assert_eq!(decode_done(&encode_done(&done).unwrap()).unwrap(), done);
    }
}
