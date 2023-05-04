use std::collections::BTreeMap;
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;

use crate::Statistic;

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
        "glog_info_messages" |
        "glog_warning_message" |
        "glog_error_messages" => {
            let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
            let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
            statistics
                .entry(( hostname.to_string(), sample.metric.clone(), metric_type.to_string(), "".to_string() ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{}: {} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                } )
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        ..Default::default()
                    }
                );
        },
        "log_bytes_logged" |
        "log_reader_bytes_read" |
        "log_sync_latency_count" |
        "log_sync_latency_sum" |
        "log_append_latency_count" |
        "log_append_latency_sum" |
        "log_cache_disk_reads" |
        "rocksdb_flush_write_bytes" |
        "intentsdb_rocksdb_flush_write_bytes" |
        "rocksdb_compact_read_bytes" |
        "intentsdb_rocksdb_compact_read_bytes" |
        "rocksdb_compact_write_bytes" |
        "intentsdb_rocksdb_compact_write_bytes" |
        "rocksdb_write_raw_block_micros_count" |
        "rocksdb_write_raw_block_micros_sum" |
        "rocksdb_sst_read_micros_count" |
        "rocksdb_sst_read_micros_sum" => {
            let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
            let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
            let table_id = sample.labels.iter().find(|(label, _)| *label == "table_id").map(|(_, value)| value).unwrap();
            statistics
                .entry(( hostname.to_string(), sample.metric.clone(), metric_type.to_string(), table_id.to_string() ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    debug!("{} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                } )
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        ..Default::default()
                    }
                );
        },
        &_ => {},
    }
}

pub fn print_yb_io(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").count() > 0
        {
            let info_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let warning_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_warning_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let error_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_error_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let guaranteed_last_timestamp = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            // I see random use of metric_type table and tablet, so all I can do is not filter a specific one and take them both.
            // I hope there is no double counting in that....
            let log_bytes_logged: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_bytes_logged" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_reader_bytes_read: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_reader_bytes_read" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_flush_write_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_flush_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let intentsdb_rocksdb_flush_write_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_flush_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_compact_read_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_compact_read_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let intentsdb_rocksdb_compact_read_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_compact_read_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_compact_write_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_compact_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let intentsdb_rocksdb_compact_write_bytes: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_compact_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_cache_disk_reads: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_cache_disk_reads" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_sync_latency_count: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_sync_latency_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_sync_latency_sum: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_sync_latency_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_append_latency_count: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_append_latency_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_append_latency_sum: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_append_latency_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_write_raw_block_micros_count: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_write_raw_block_micros_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_write_raw_block_micros_sum: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_write_raw_block_micros_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_sst_read_micros_count: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_sst_read_micros_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_sst_read_micros_sum: f64 = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_sst_read_micros_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();

            println!("{:50} {:8} {:10.2} {:10.2}|{:10.2} {:10.2} {:10.2} {:10.2} {:10.2} {:10.2}|{:10.2}|{:10.2} {:10.2}|{:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     guaranteed_last_timestamp.format("%H:%M:%S"),
                     info_messages,
                     warning_messages + error_messages,
                     log_bytes_logged / (1024.*1024.),
                     log_reader_bytes_read / (1024.*1024.),
                     log_append_latency_count,
                     log_cache_disk_reads,
                     if (( log_append_latency_sum / log_append_latency_count) / 1000.).is_nan() {
                         0.
                     } else {
                         ( log_append_latency_sum / log_append_latency_count) / 1000.
                     },
                     if (( log_sync_latency_sum / log_sync_latency_count) / 1000.).is_nan() {
                         0.
                     } else {
                         ( log_sync_latency_sum / log_sync_latency_count) / 1000.
                     },
                     (rocksdb_flush_write_bytes + intentsdb_rocksdb_flush_write_bytes) / (1024.*1024.),
                     (rocksdb_compact_read_bytes + intentsdb_rocksdb_compact_read_bytes) / (1024.*1024.),
                     (rocksdb_compact_write_bytes + intentsdb_rocksdb_compact_write_bytes) / (1024.*1024.),
                     rocksdb_write_raw_block_micros_count,
                     if (( rocksdb_write_raw_block_micros_sum / rocksdb_write_raw_block_micros_count) / 1000.).is_nan() {
                         0.
                     } else {
                         ( rocksdb_write_raw_block_micros_sum / rocksdb_write_raw_block_micros_count) / 1000.
                     },
                     rocksdb_sst_read_micros_count,
                     if (( rocksdb_sst_read_micros_sum / rocksdb_sst_read_micros_count) / 1000.).is_nan() {
                         0.
                     } else {
                         ( rocksdb_sst_read_micros_sum / rocksdb_sst_read_micros_count) / 1000.
                     },
            );
        }
    }
}

pub fn print_yb_io_header()
{
    println!("{:50} {:8} {:10} {:>10}|{:10} {:10} {:10} {:10} {:10} {:>10}|{:>10}|{:10} {:>10}|{:10} {:10} {:10} {:>10}",
             "",
             "",
             "",
             "glog",
             "",
             "",
             "",
             "",
             "",
             "log",
             "flush",
             "",
             "compaction",
             "",
             "",
             "",
             "rocksdb",
    );
    println!("{:50} {:8} {:>10} {:>10}|{:>10} {:>10} {:>10} {:>10} {:>10} {:>10}|{:>10}|{:>10} {:>10}|{:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "info",
             "warn+err",
             "W_MBPS",
             "R_MBPS",
             "W_IOPS",
             "R_IOPS",
             "W_lat(ms)",
             "sync_lat",
             "W_MBPS",
             "R_MBPS",
             "W_MBPS",
             "W_IOPS",
             "W_lat(ms)",
             "R_IOPS",
             "R_lat(ms)",
    );
}
