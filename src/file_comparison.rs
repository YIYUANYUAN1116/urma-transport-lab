//! B4 file-to-file comparison orchestration and transport-neutral reporting.

use crate::{
    run_tcp_child, run_tcp_parent, BenchmarkCase, BenchmarkResult, BenchmarkScenario,
    BenchmarkTransport, Error, FileCompletionPolicy, FileSource, Result, TcpBenchmarkDestination,
    TcpBenchmarkSource, TimingMode,
};
use std::{cmp::Ordering, collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct B4FileMatrixConfig {
    pub case_id: String,
    pub input_path: PathBuf,
    pub transfer_bytes: u64,
    pub chunk_size: u64,
    pub window: u32,
    pub repeat_count: u32,
    pub timing_mode: TimingMode,
    pub completion_policy: FileCompletionPolicy,
    pub data_seed: u64,
    pub transports: Vec<BenchmarkTransport>,
}

impl B4FileMatrixConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        case_id: impl Into<String>,
        input_path: impl Into<PathBuf>,
        transfer_bytes: u64,
        chunk_size: u64,
        window: u32,
        repeat_count: u32,
        timing_mode: TimingMode,
        completion_policy: FileCompletionPolicy,
        data_seed: u64,
        transports: Vec<BenchmarkTransport>,
    ) -> Result<Self> {
        let config = Self {
            case_id: case_id.into(),
            input_path: input_path.into(),
            transfer_bytes,
            chunk_size,
            window,
            repeat_count,
            timing_mode,
            completion_policy,
            data_seed,
            transports,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.repeat_count == 0 {
            return Err(invalid("B4 repeat_count must be non-zero"));
        }
        if self.input_path.as_os_str().is_empty() {
            return Err(invalid("B4 input file path must not be empty"));
        }
        if self.transports.is_empty() {
            return Err(invalid("B4 requires at least one transport"));
        }
        let mut seen = Vec::new();
        for &transport in &self.transports {
            if seen.contains(&transport) {
                return Err(invalid(format!(
                    "duplicate B4 transport {}",
                    transport.as_str()
                )));
            }
            seen.push(transport);
            BenchmarkCase::new(
                self.case_id.clone(),
                1,
                BenchmarkScenario::File,
                transport,
                self.transfer_bytes,
                self.chunk_size,
                self.window,
                self.timing_mode,
                self.completion_policy,
                self.data_seed,
            )?;
        }
        Ok(())
    }

    pub fn cases(&self) -> Result<Vec<BenchmarkCase>> {
        self.validate()?;
        let capacity = self
            .transports
            .len()
            .checked_mul(self.repeat_count as usize)
            .ok_or_else(|| invalid("B4 case count overflow"))?;
        let mut cases = Vec::with_capacity(capacity);
        for &transport in &self.transports {
            for repeat in 1..=self.repeat_count {
                cases.push(BenchmarkCase::new(
                    format!("{}-{}", self.case_id, transport.as_str()),
                    repeat,
                    BenchmarkScenario::File,
                    transport,
                    self.transfer_bytes,
                    self.chunk_size,
                    self.window,
                    self.timing_mode,
                    self.completion_policy,
                    self.data_seed,
                )?);
            }
        }
        Ok(cases)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum B4TransportDispatch {
    TcpUserspace,
    TcpSendfile,
    Urma,
}

impl B4TransportDispatch {
    pub fn for_case(case: &BenchmarkCase) -> Result<Self> {
        case.validate()?;
        if case.scenario != BenchmarkScenario::File {
            return Err(invalid("B4 supports only file-to-file cases"));
        }
        Ok(match case.transport {
            BenchmarkTransport::TcpUserspace => Self::TcpUserspace,
            BenchmarkTransport::TcpSendfile => Self::TcpSendfile,
            BenchmarkTransport::Urma => Self::Urma,
        })
    }
}

pub fn dispatch_b4_file_parent(
    case: &BenchmarkCase,
    listen: &str,
    source: FileSource,
    device: &str,
    eid_index: u32,
) -> Result<BenchmarkResult> {
    match B4TransportDispatch::for_case(case)? {
        B4TransportDispatch::TcpUserspace | B4TransportDispatch::TcpSendfile => {
            run_tcp_parent(case, listen, TcpBenchmarkSource::File(source))
        }
        B4TransportDispatch::Urma => {
            #[cfg(feature = "urma")]
            {
                crate::run_urma_parent(
                    case,
                    device.to_owned(),
                    eid_index,
                    listen,
                    crate::UrmaBenchmarkSource::File(source),
                )
            }
            #[cfg(not(feature = "urma"))]
            {
                let _ = (source, device, eid_index, listen);
                Err(Error::FeatureDisabled)
            }
        }
    }
}

pub fn dispatch_b4_file_child(
    case: &BenchmarkCase,
    parent: &str,
    output_path: impl Into<PathBuf>,
    device: &str,
    eid_index: u32,
) -> Result<BenchmarkResult> {
    let output_path = output_path.into();
    match B4TransportDispatch::for_case(case)? {
        B4TransportDispatch::TcpUserspace | B4TransportDispatch::TcpSendfile => {
            run_tcp_child(case, parent, TcpBenchmarkDestination::File(output_path))
        }
        B4TransportDispatch::Urma => {
            #[cfg(feature = "urma")]
            {
                crate::run_urma_child(
                    case,
                    device.to_owned(),
                    eid_index,
                    parent,
                    crate::UrmaBenchmarkDestination::File(output_path),
                )
            }
            #[cfg(not(feature = "urma"))]
            {
                let _ = (output_path, device, eid_index, parent);
                Err(Error::FeatureDisabled)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum B4CaseStatus {
    Success,
    IntegrityFailure,
    Unsupported,
    Failed,
}

impl B4CaseStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::IntegrityFailure => "integrity-failure",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct B4FileRunRecord {
    pub case: BenchmarkCase,
    pub status: B4CaseStatus,
    pub result: Option<BenchmarkResult>,
    pub detail: Option<String>,
}

impl B4FileRunRecord {
    pub fn success(case: BenchmarkCase, result: BenchmarkResult) -> Self {
        match validate_b4_result(&case, &result) {
            Ok(()) => Self {
                case,
                status: B4CaseStatus::Success,
                result: Some(result),
                detail: None,
            },
            Err(detail) => Self {
                case,
                status: if result.integrity.is_ok() {
                    B4CaseStatus::Failed
                } else {
                    B4CaseStatus::IntegrityFailure
                },
                result: Some(result),
                detail: Some(detail),
            },
        }
    }

    pub fn unsupported(case: BenchmarkCase, detail: impl Into<String>) -> Self {
        Self {
            case,
            status: B4CaseStatus::Unsupported,
            result: None,
            detail: Some(detail.into()),
        }
    }

    pub fn failed(case: BenchmarkCase, detail: impl Into<String>) -> Self {
        Self {
            case,
            status: B4CaseStatus::Failed,
            result: None,
            detail: Some(detail.into()),
        }
    }

    pub fn to_json_line(&self) -> String {
        if self.status == B4CaseStatus::Success {
            return self
                .result
                .as_ref()
                .expect("success has result")
                .to_json_line();
        }
        format!(
            "{{\"record_type\":\"raw\",\"case_id\":\"{}\",\"repeat\":{},\"transport\":\"{}\",\"status\":\"{}\",\"detail\":\"{}\"}}",
            escape_json(&self.case.case_id),
            self.case.repeat,
            self.case.transport.as_str(),
            self.status.as_str(),
            escape_json(self.detail.as_deref().unwrap_or(""))
        )
    }
}

pub enum B4ExecutionOutcome {
    Result(BenchmarkResult),
    Unsupported(String),
    Failed(String),
}

pub struct B4FileMatrixRunner {
    config: B4FileMatrixConfig,
    source: FileSource,
    cases: Vec<BenchmarkCase>,
}

impl B4FileMatrixRunner {
    pub fn prepare(config: B4FileMatrixConfig) -> Result<Self> {
        config.validate()?;
        let source = FileSource::from_path(&config.input_path)?;
        if source.length() != config.transfer_bytes {
            return Err(invalid(format!(
                "B4 source length {} does not match transfer_bytes {}",
                source.length(),
                config.transfer_bytes
            )));
        }
        let cases = config.cases()?;
        Ok(Self {
            config,
            source,
            cases,
        })
    }

    pub fn config(&self) -> &B4FileMatrixConfig {
        &self.config
    }

    pub fn source(&self) -> &FileSource {
        &self.source
    }

    pub fn cases(&self) -> &[BenchmarkCase] {
        &self.cases
    }

    /// Executes every expanded case while borrowing the same pre-scanned
    /// FileSource metadata. The callback performs real transport work; this
    /// runner never manufactures URMA samples.
    pub fn run_with(
        &self,
        mut execute: impl FnMut(&BenchmarkCase, &FileSource) -> B4ExecutionOutcome,
    ) -> B4Report {
        let mut records = Vec::with_capacity(self.cases.len());
        for case in &self.cases {
            let record = match execute(case, &self.source) {
                B4ExecutionOutcome::Result(result) => {
                    B4FileRunRecord::success(case.clone(), result)
                }
                B4ExecutionOutcome::Unsupported(detail) => {
                    B4FileRunRecord::unsupported(case.clone(), detail)
                }
                B4ExecutionOutcome::Failed(detail) => B4FileRunRecord::failed(case.clone(), detail),
            };
            records.push(record);
        }
        B4Report::from_records(records)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct B4Aggregate {
    pub transport: BenchmarkTransport,
    pub sample_count: usize,
    pub integrity_failures: usize,
    pub unsupported_cases: usize,
    pub failed_cases: usize,
    pub throughput_mib_s_median: f64,
    pub throughput_mib_s_min: f64,
    pub throughput_mib_s_max: f64,
    pub throughput_gbit_s_median: f64,
    pub cv_percent: f64,
    pub unstable: bool,
}

impl B4Aggregate {
    pub fn to_json_line(&self) -> String {
        format!(
            "{{\"record_type\":\"aggregate\",\"transport\":\"{}\",\"sample_count\":{},\"integrity_failures\":{},\"unsupported_cases\":{},\"failed_cases\":{},\"throughput_mib_s_median\":{:.6},\"throughput_mib_s_min\":{:.6},\"throughput_mib_s_max\":{:.6},\"throughput_gbit_s_median\":{:.6},\"cv_percent\":{:.6},\"unstable\":{}}}",
            self.transport.as_str(),
            self.sample_count,
            self.integrity_failures,
            self.unsupported_cases,
            self.failed_cases,
            self.throughput_mib_s_median,
            self.throughput_mib_s_min,
            self.throughput_mib_s_max,
            self.throughput_gbit_s_median,
            self.cv_percent,
            self.unstable
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct B4Report {
    pub records: Vec<B4FileRunRecord>,
    pub aggregates: Vec<B4Aggregate>,
}

impl B4Report {
    pub fn from_records(records: Vec<B4FileRunRecord>) -> Self {
        let mut grouped: BTreeMap<&'static str, (BenchmarkTransport, Vec<&B4FileRunRecord>)> =
            BTreeMap::new();
        for record in &records {
            grouped
                .entry(record.case.transport.as_str())
                .or_insert_with(|| (record.case.transport, Vec::new()))
                .1
                .push(record);
        }
        let aggregates = grouped
            .into_values()
            .map(|(transport, records)| aggregate(transport, &records))
            .collect();
        Self {
            records,
            aggregates,
        }
    }

    pub fn json_lines(&self) -> Vec<String> {
        self.records
            .iter()
            .map(B4FileRunRecord::to_json_line)
            .chain(self.aggregates.iter().map(B4Aggregate::to_json_line))
            .collect()
    }

    pub fn to_csv(&self) -> String {
        let mut output = String::from(
            "record_type,status,case_id,repeat,transport,bytes,chunk_size,window,timing_mode,completion_policy,elapsed_ns,parent_elapsed_ns,throughput_mib_s,throughput_gbit_s,integrity_ok,detail\n",
        );
        for record in &self.records {
            let result = record.result.as_ref();
            let parent_elapsed =
                result.and_then(|value| value.transport_stats.get("parent_elapsed_ns").copied());
            output.push_str(&format!(
                "raw,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                record.status.as_str(),
                csv_field(&record.case.case_id),
                record.case.repeat,
                record.case.transport.as_str(),
                result.map_or(record.case.transfer_bytes, |value| value.bytes),
                record.case.chunk_size,
                record.case.window,
                record.case.timing_mode.as_str(),
                record.case.completion_policy.as_str(),
                optional_u64(result.map(|value| value.elapsed_ns)),
                optional_u64(parent_elapsed),
                optional_f64(result.map(|value| value.throughput_mib_s)),
                optional_f64(result.map(|value| value.throughput_gbit_s)),
                result.is_some_and(|value| value.integrity.is_ok()),
                csv_field(record.detail.as_deref().unwrap_or("")),
            ));
        }
        for aggregate in &self.aggregates {
            let detail = csv_field(&format!(
                    "samples={};min_mib_s={:.6};max_mib_s={:.6};cv_percent={:.6};integrity_failures={};unsupported={};failed={}",
                    aggregate.sample_count,
                    aggregate.throughput_mib_s_min,
                    aggregate.throughput_mib_s_max,
                    aggregate.cv_percent,
                    aggregate.integrity_failures,
                    aggregate.unsupported_cases,
                    aggregate.failed_cases
                ));
            let fields = vec![
                "aggregate".to_owned(),
                if aggregate.unstable {
                    "unstable".to_owned()
                } else {
                    "stable".to_owned()
                },
                String::new(),
                String::new(),
                aggregate.transport.as_str().to_owned(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("{:.6}", aggregate.throughput_mib_s_median),
                format!("{:.6}", aggregate.throughput_gbit_s_median),
                String::new(),
                detail,
            ];
            output.push_str(&fields.join(","));
            output.push('\n');
        }
        output
    }
}

fn validate_b4_result(
    case: &BenchmarkCase,
    result: &BenchmarkResult,
) -> std::result::Result<(), String> {
    if result.case_id != case.case_id
        || result.repeat != case.repeat
        || result.transport != case.transport
        || result.scenario != BenchmarkScenario::File
        || result.chunk_size != case.chunk_size
        || result.window != case.window
        || result.timing_mode != case.timing_mode
        || result.completion_policy != case.completion_policy
    {
        return Err("result does not match B4 case identity/parameters".into());
    }
    if !result.integrity.is_ok() {
        return Err("length or CRC32 integrity failed".into());
    }
    let stat = |name: &str| {
        result
            .transport_stats
            .get(name)
            .copied()
            .ok_or_else(|| format!("missing required transport stat {name}"))
    };
    if stat("bytes_sent")? != case.transfer_bytes || stat("bytes_received")? != case.transfer_bytes
    {
        return Err("transport byte counters do not match transfer_bytes".into());
    }
    stat("parent_elapsed_ns")?;
    match case.transport {
        BenchmarkTransport::TcpUserspace => {
            for name in [
                "parent_read_calls",
                "parent_write_calls",
                "child_read_calls",
                "partial_write_count",
            ] {
                stat(name)?;
            }
        }
        BenchmarkTransport::TcpSendfile => {
            for name in ["sendfile_calls", "partial_sendfile_count"] {
                stat(name)?;
            }
            if stat("parent_read_calls")? != 0 || stat("parent_write_calls")? != 0 {
                return Err("tcp-sendfile used Parent userspace read/write".into());
            }
        }
        BenchmarkTransport::Urma => {
            for name in [
                "send_post",
                "recv_post",
                "send_cqe",
                "send_retired",
                "recv_cqe",
                "cqe_error",
                "poll_calls",
                "empty_polls",
                "send_jfc_poll_calls",
                "recv_jfc_poll_calls",
                "yield_count",
                "sleep_count",
                "backoff_sleep_ns",
                "jfc_rearm_count",
                "event_wait_count",
                "event_wakeup_count",
                "event_timeout_count",
                "spurious_wakeup_count",
                "event_wait_ns",
                "max_event_wait_ns",
                "max_empty_streak",
                "nonempty_polls",
                "completion_batch_total",
                "avg_poll_batch_milli",
                "empty_poll_ratio_ppm",
                "max_completion_poll_gap_ns",
                "max_outstanding_send",
                "current_outstanding_send",
                "current_outstanding_recv",
                "configured_window",
                "configured_receive_credit",
                "slot_size",
                "effective_payload_size",
                "tx_slot_count",
                "rx_slot_count",
                "total_registered_bytes",
            ] {
                stat(name)?;
            }
            if stat("current_outstanding_send")? != 0
                || stat("current_outstanding_recv")? != 0
                || stat("send_post")? != stat("send_retired")?
                || stat("recv_post")? != stat("recv_cqe")?
                || stat("cqe_error")? != 0
                || stat("configured_window")? != u64::from(case.window)
                || stat("effective_payload_size")? != case.chunk_size
            {
                return Err("URMA completion/window/slot accounting did not close".into());
            }
            if case.window > 1
                && case.chunk_count().map_err(|error| error.to_string())? >= 2
                && stat("max_outstanding_send")? <= 1
            {
                return Err("URMA W>1 case did not achieve multiple outstanding SENDs".into());
            }
        }
    }
    Ok(())
}

fn aggregate(transport: BenchmarkTransport, records: &[&B4FileRunRecord]) -> B4Aggregate {
    let successful: Vec<&BenchmarkResult> = records
        .iter()
        .filter(|record| record.status == B4CaseStatus::Success)
        .filter_map(|record| record.result.as_ref())
        .collect();
    let mib: Vec<f64> = successful
        .iter()
        .map(|result| result.throughput_mib_s)
        .collect();
    let gbit: Vec<f64> = successful
        .iter()
        .map(|result| result.throughput_gbit_s)
        .collect();
    let cv_percent = coefficient_of_variation(&mib);
    B4Aggregate {
        transport,
        sample_count: successful.len(),
        integrity_failures: records
            .iter()
            .filter(|record| record.status == B4CaseStatus::IntegrityFailure)
            .count(),
        unsupported_cases: records
            .iter()
            .filter(|record| record.status == B4CaseStatus::Unsupported)
            .count(),
        failed_cases: records
            .iter()
            .filter(|record| record.status == B4CaseStatus::Failed)
            .count(),
        throughput_mib_s_median: median(&mib),
        throughput_mib_s_min: mib.iter().copied().reduce(f64::min).unwrap_or(0.0),
        throughput_mib_s_max: mib.iter().copied().reduce(f64::max).unwrap_or(0.0),
        throughput_gbit_s_median: median(&gbit),
        cv_percent,
        unstable: successful.len() >= 5 && cv_percent > 5.0,
    }
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn coefficient_of_variation(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean * 100.0
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn optional_f64(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| format!("{value:.6}"))
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn escape_json(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            value if value.is_control() => output.push_str(&format!("\\u{:04x}", value as u32)),
            value => output.push(value),
        }
    }
    output
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidConfiguration(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{crc32_bytes, BufferPoolConfig, IntegrityResult, TimingSample, UrmaPipelineLimits};
    use std::{fs, path::Path, time::Duration};

    struct TempFile(PathBuf);

    impl TempFile {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "urma-transport-lab-b4-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn config(path: &Path, bytes: u64, repeats: u32) -> B4FileMatrixConfig {
        B4FileMatrixConfig::new(
            "b4-test",
            path,
            bytes,
            256 * 1024,
            4,
            repeats,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            42,
            vec![
                BenchmarkTransport::TcpUserspace,
                BenchmarkTransport::TcpSendfile,
                BenchmarkTransport::Urma,
            ],
        )
        .unwrap()
    }

    fn result(case: &BenchmarkCase, throughput: f64) -> BenchmarkResult {
        let integrity = IntegrityResult::new(
            case.transfer_bytes,
            case.transfer_bytes,
            crc32_bytes(b"same"),
            crc32_bytes(b"same"),
        );
        let mut result = BenchmarkResult::from_sample(
            case,
            TimingSample::from_duration(case.timing_mode, Duration::from_millis(10)),
            integrity,
        )
        .unwrap();
        result.throughput_mib_s = throughput;
        result.throughput_gbit_s = throughput * 0.008_388_608;
        result
            .transport_stats
            .insert("bytes_sent".into(), case.transfer_bytes);
        result
            .transport_stats
            .insert("bytes_received".into(), case.transfer_bytes);
        result
            .transport_stats
            .insert("parent_elapsed_ns".into(), 9_000_000);
        match case.transport {
            BenchmarkTransport::TcpUserspace => {
                for name in [
                    "parent_read_calls",
                    "parent_write_calls",
                    "child_read_calls",
                    "partial_write_count",
                ] {
                    result.transport_stats.insert(name.into(), 1);
                }
            }
            BenchmarkTransport::TcpSendfile => {
                result.transport_stats.insert("sendfile_calls".into(), 1);
                result
                    .transport_stats
                    .insert("partial_sendfile_count".into(), 0);
                result.transport_stats.insert("parent_read_calls".into(), 0);
                result
                    .transport_stats
                    .insert("parent_write_calls".into(), 0);
            }
            BenchmarkTransport::Urma => {
                for (name, value) in [
                    ("send_post", 6),
                    ("recv_post", 6),
                    ("send_cqe", 6),
                    ("send_retired", 6),
                    ("recv_cqe", 6),
                    ("cqe_error", 0),
                    ("poll_calls", 3),
                    ("empty_polls", 0),
                    ("send_jfc_poll_calls", 3),
                    ("recv_jfc_poll_calls", 3),
                    ("yield_count", 0),
                    ("sleep_count", 0),
                    ("backoff_sleep_ns", 0),
                    ("jfc_rearm_count", 2),
                    ("event_wait_count", 1),
                    ("event_wakeup_count", 1),
                    ("event_timeout_count", 0),
                    ("spurious_wakeup_count", 0),
                    ("event_wait_ns", 5_000),
                    ("max_event_wait_ns", 5_000),
                    ("max_empty_streak", 0),
                    ("nonempty_polls", 3),
                    ("completion_batch_total", 12),
                    ("avg_poll_batch_milli", 4_000),
                    ("empty_poll_ratio_ppm", 0),
                    ("max_completion_poll_gap_ns", 10_000),
                    ("max_outstanding_send", u64::from(case.window)),
                    ("current_outstanding_send", 0),
                    ("current_outstanding_recv", 0),
                    ("configured_window", u64::from(case.window)),
                    ("configured_receive_credit", u64::from(case.window)),
                    ("slot_size", 266_240),
                    ("effective_payload_size", case.chunk_size),
                    ("tx_slot_count", 8),
                    ("rx_slot_count", 8),
                    ("total_registered_bytes", 4_259_840),
                ] {
                    result.transport_stats.insert(name.into(), value);
                }
            }
        }
        result
    }

    #[test]
    fn constructs_and_dispatches_three_file_transports() {
        let path = TempFile::new("dispatch");
        FileSource::generate(path.path(), 17, 42, 7).unwrap();
        let runner = B4FileMatrixRunner::prepare(config(path.path(), 17, 2)).unwrap();
        assert_eq!(runner.cases().len(), 6);
        assert_eq!(runner.source().length(), 17);
        assert_eq!(
            runner.source().expected_crc32(),
            crc32_bytes(&runner.source_bytes())
        );
        assert_eq!(
            B4TransportDispatch::for_case(&runner.cases()[0]).unwrap(),
            B4TransportDispatch::TcpUserspace
        );
        assert_eq!(
            B4TransportDispatch::for_case(&runner.cases()[2]).unwrap(),
            B4TransportDispatch::TcpSendfile
        );
        assert_eq!(
            B4TransportDispatch::for_case(&runner.cases()[4]).unwrap(),
            B4TransportDispatch::Urma
        );
    }

    #[test]
    fn reuses_one_file_source_metadata_for_all_repeats() {
        let path = TempFile::new("reuse");
        FileSource::generate(path.path(), 65_537, 9, 4096).unwrap();
        let runner = B4FileMatrixRunner::prepare(config(path.path(), 65_537, 2)).unwrap();
        let expected_ptr = runner.source() as *const FileSource;
        let mut calls = 0;
        let report = runner.run_with(|case, source| {
            calls += 1;
            assert_eq!(source as *const FileSource, expected_ptr);
            if case.transport == BenchmarkTransport::Urma {
                B4ExecutionOutcome::Unsupported("no provider in unit test".into())
            } else {
                B4ExecutionOutcome::Result(result(case, 100.0 + f64::from(case.repeat)))
            }
        });
        assert_eq!(calls, 6);
        assert_eq!(report.records.len(), 6);
        assert_eq!(report.aggregates.len(), 3);
        assert_eq!(
            report
                .aggregates
                .iter()
                .find(|value| value.transport == BenchmarkTransport::Urma)
                .unwrap()
                .unsupported_cases,
            2
        );
    }

    #[test]
    fn supports_zero_non_multiple_and_both_completion_policies() {
        for (bytes, policy) in [
            (0, FileCompletionPolicy::Buffered),
            (65_537, FileCompletionPolicy::Durable),
        ] {
            let path = TempFile::new("edge");
            FileSource::generate(path.path(), bytes, 5, 4096).unwrap();
            let mut candidate = config(path.path(), bytes, 1);
            candidate.completion_policy = policy;
            let runner = B4FileMatrixRunner::prepare(candidate).unwrap();
            assert_eq!(runner.source().length(), bytes);
            assert!(runner
                .cases()
                .iter()
                .all(|case| case.completion_policy == policy));
        }
    }

    #[test]
    fn classifies_integrity_failure_and_unsupported_scenario() {
        let case = BenchmarkCase::new(
            "bad",
            1,
            BenchmarkScenario::File,
            BenchmarkTransport::TcpUserspace,
            4,
            3,
            1,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            0,
        )
        .unwrap();
        let mut bad = result(&case, 1.0);
        bad.integrity.actual_crc32 ^= 1;
        bad.integrity.digest_ok = false;
        assert_eq!(
            B4FileRunRecord::success(case, bad).status,
            B4CaseStatus::IntegrityFailure
        );

        let memory = BenchmarkCase::new(
            "memory",
            1,
            BenchmarkScenario::Memory,
            BenchmarkTransport::TcpUserspace,
            0,
            1,
            1,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            0,
        )
        .unwrap();
        assert!(B4TransportDispatch::for_case(&memory).is_err());
    }

    #[test]
    fn propagates_urma_chunk_window_and_slot_validation() {
        let path = TempFile::new("urma-params");
        FileSource::generate(path.path(), 600_001, 4, 4096).unwrap();
        let runner = B4FileMatrixRunner::prepare(config(path.path(), 600_001, 1)).unwrap();
        let case = runner
            .cases()
            .iter()
            .find(|case| case.transport == BenchmarkTransport::Urma)
            .unwrap();
        assert_eq!(case.chunk_size, 256 * 1024);
        assert_eq!(case.window, 4);
        let slot = crate::derive_urma_slot_size(case, 4096).unwrap();
        crate::validate_urma_case(
            case,
            UrmaPipelineLimits {
                slot_size: slot,
                tx_slot_count: 8,
                rx_slot_count: 8,
                send_jfc_depth: 64,
                recv_jfc_depth: 64,
                jetty_send_depth: 64,
                jetty_recv_depth: 64,
                provider_max_message_size: slot as u64,
            },
        )
        .unwrap();
        assert_eq!(BufferPoolConfig::default().tx_slot_count, 128);
        assert_eq!(BufferPoolConfig::default().rx_slot_count, 512);
        assert_eq!(
            B4FileRunRecord::success(case.clone(), result(case, 10.0)).status,
            B4CaseStatus::Success
        );
    }

    #[test]
    fn aggregates_median_min_max_cv_and_marks_unstable() {
        let cases: Vec<_> = (1..=5)
            .map(|repeat| {
                BenchmarkCase::new(
                    "aggregate",
                    repeat,
                    BenchmarkScenario::File,
                    BenchmarkTransport::TcpUserspace,
                    1024,
                    256,
                    1,
                    TimingMode::SteadyState,
                    FileCompletionPolicy::Buffered,
                    0,
                )
                .unwrap()
            })
            .collect();
        let values = [80.0, 90.0, 100.0, 110.0, 120.0];
        let records = cases
            .into_iter()
            .zip(values)
            .map(|(case, value)| B4FileRunRecord::success(case.clone(), result(&case, value)))
            .collect();
        let report = B4Report::from_records(records);
        let aggregate = &report.aggregates[0];
        assert_eq!(aggregate.throughput_mib_s_median, 100.0);
        assert_eq!(aggregate.throughput_mib_s_min, 80.0);
        assert_eq!(aggregate.throughput_mib_s_max, 120.0);
        assert!(aggregate.cv_percent > 5.0);
        assert!(aggregate.unstable);
    }

    #[test]
    fn result_json_and_csv_are_stable() {
        let case = BenchmarkCase::new(
            "csv,case",
            1,
            BenchmarkScenario::File,
            BenchmarkTransport::TcpUserspace,
            1024,
            256,
            1,
            TimingMode::SetupIncluded,
            FileCompletionPolicy::Durable,
            0,
        )
        .unwrap();
        let report = B4Report::from_records(vec![B4FileRunRecord::success(
            case.clone(),
            result(&case, 123.0),
        )]);
        let lines = report.json_lines();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"throughput_gbit_s\":"));
        assert!(lines[1].contains("\"record_type\":\"aggregate\""));
        let csv = report.to_csv();
        assert!(csv.starts_with("record_type,status,case_id"));
        assert!(csv.contains("\"csv,case\""));
        assert_eq!(csv.lines().count(), 3);
        assert!(csv.contains("aggregate,stable,,,tcp-userspace"));
    }

    trait SourceBytes {
        fn source_bytes(&self) -> Vec<u8>;
    }

    impl SourceBytes for B4FileMatrixRunner {
        fn source_bytes(&self) -> Vec<u8> {
            let mut file = self.source().open().unwrap();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut bytes).unwrap();
            bytes
        }
    }
}
