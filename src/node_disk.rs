use std::collections::BTreeMap;
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;

use crate::Statistic;

#[derive(Debug)]
pub struct NodeDiskDetails {
    pub disk: String,
    pub reads_completed_s: f64,
    pub reads_bytes_s: f64,
    pub reads_avg_latency_s: f64,
    pub reads_merged_s: f64,
    pub writes_completed_s: f64,
    pub writes_bytes_s: f64,
    pub writes_avg_latency_s: f64,
    pub writes_merged_s: f64,
    pub discards_completed_s: f64,
    pub discards_sectors_s: f64,
    pub discards_avg_latency: f64,
    pub discards_merged_s: f64,
    pub queue_size: f64,
}

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric) };
    let device = sample.labels.iter().find(|(label, _)| *label == "device").map(|(_, value)| value).unwrap();
    // do not store device mapper disk statistics
    if device.starts_with("dm-") { return; };
    statistics
        .entry((hostname.to_string(), sample.metric.clone(), device.to_string(), "".to_string()))
        .and_modify(|row| {
            row.delta_value = value - row.last_value;
            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
            row.last_value = value;
            row.last_timestamp = sample.timestamp;
            debug!("{} device: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, device, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
        })
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                ..Default::default()
            }
        );
}

pub fn create_total(
    sample: &Sample,
    host: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
        "node_disk_read_bytes_total" |
        "node_disk_read_time_seconds_total" |
        "node_disk_reads_completed_total" |
        "node_disk_reads_merged_total" |
        "node_disk_written_bytes_total" |
        "node_disk_write_time_seconds_total" |
        "node_disk_writes_completed_total" |
        "node_disk_writes_merged_total" |
        "node_disk_discarded_sectors_total" |
        "node_disk_discard_time_seconds_total" |
        "node_disk_discards_completed_total" |
        "node_disk_discards_merged_total" |
        "node_disk_io_time_seconds_total" |
        "node_disk_io_time_weighted_seconds_total" |
        "node_xfs_read_calls_total" |
        "node_xfs_write_calls_total" => {
            let last_timestamp = statistics.iter().find(|((hostname, metric, device, _), _)| hostname == host && metric == &sample.metric && device != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                let per_second_value = statistics.iter().filter(|((hostname, metric, device, _), _)| hostname == host && metric == &sample.metric && device != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), "".to_string()))
                    .and_modify(|row| {
                        row.per_second_value = per_second_value;
                        row.last_timestamp = last_timestamp;
                    })
                    .or_insert(Statistic { per_second_value, last_timestamp, ..Default::default() });
        },
        &_ => {},
    }
}

pub fn print_sar_d(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").map(|((_, _, device, _), _)| device)
            {
                let reads_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let writes_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let tps = reads_completed + writes_completed;
                let read_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let write_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let mut average_read_request_size = read_bytes / reads_completed;
                average_read_request_size = if average_read_request_size.is_nan() { 0. } else { average_read_request_size };
                let mut average_write_request_size = write_bytes / writes_completed;
                average_write_request_size = if average_write_request_size.is_nan() { 0. } else { average_write_request_size };
                let queue_size = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_io_time_weighted_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let read_time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let write_time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_write_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let mut average_read_request_time_ms = (read_time * 1000.) / reads_completed;
                average_read_request_time_ms = if average_read_request_time_ms.is_nan() { 0. } else { average_read_request_time_ms };
                let mut average_write_request_time_ms = (write_time * 1000.) / writes_completed;
                average_write_request_time_ms = if average_write_request_time_ms.is_nan() { 0. } else { average_write_request_time_ms };
                let time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                println!("{:30} {:8} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         tps,
                         read_bytes / (1024. * 1024.),
                         write_bytes / (1024. * 1024.),
                         (average_read_request_size + average_write_request_size) / (1024. * 1024.),
                         queue_size,
                         (average_read_request_time_ms + average_write_request_time_ms) / 2.
                );
            }
        }
    }
}

pub fn print_sar_d_header()
{
    println!("{:30} {:8} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
             "time",
             "DEV",
             "tps",
             "rMB/s",
             "wMB/s",
             "areq-sz",
             "aqu-sz",
             "await",
    );
}

pub fn print_xfs_iops(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_xfs_read_calls_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_xfs_read_calls_total").map(|((_, _, device, _), _)| device)
            {
                let reads_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_xfs_read_calls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let writes_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_xfs_write_calls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let time = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_xfs_read_calls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                println!("{:30} {:8} {:10} {:10.2} {:10.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         writes_completed,
                         reads_completed,
                );
            }
        }
    }
}

pub fn print_xfs_iops_header()
{
    println!("{:30} {:8} {:10} {:>10} {:>10}",
             "hostname",
             "time",
             "dev",
             "W_IOPS",
             "R_IOPS",
    );
}

pub fn print_iostat(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").map(|((_, _, device, _), _)| device)
            {
                let reads_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let writes_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let tps = reads_completed + writes_completed;
                let read_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let write_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let read_total = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.delta_value).next().unwrap();
                let write_total = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.delta_value).next().unwrap();
                let time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                println!("{:30} {:8} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         tps, read_bytes / (1024. * 1024.),
                         write_bytes / (1024. * 1024.),
                         read_total / (1024. * 1024.),
                         write_total / (1024. * 1024.),
                );
            }
        }
    }
}

pub fn print_iostat_header()
{
    println!("{:30} {:8} {:10} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
             "time",
             "Device",
             "tps",
             "MB_read/s",
             "MB_wrtn/s",
             "MB_read",
             "MB_wrtn",
    );
}

pub fn print_iostat_x(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").map(|((_, _, device, _), _)| device)
            {
                let reads_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let writes_completed = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let read_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let write_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let reads_merged = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_merged_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let writes_merged = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_merged_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let read_time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let write_time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_write_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let queue = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_write_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let mut read_percentage_merged = reads_merged / (reads_merged + reads_completed) * 100.;
                read_percentage_merged = if read_percentage_merged.is_nan() { 0. } else { read_percentage_merged };
                let mut write_percentage_merged = writes_merged / (writes_merged + writes_completed) * 100.;
                write_percentage_merged = if write_percentage_merged.is_nan() { 0. } else { write_percentage_merged };
                let mut read_average_time_ms = (read_time * 1000.) / reads_completed;
                read_average_time_ms = if read_average_time_ms.is_nan() { 0. } else { read_average_time_ms };
                let mut write_average_time_ms = (write_time * 1000.) / writes_completed;
                write_average_time_ms = if write_average_time_ms.is_nan() { 0. } else { write_average_time_ms };
                let mut read_average_request_size = read_bytes / reads_completed;
                read_average_request_size = if read_average_request_size.is_nan() { 0. } else { read_average_request_size };
                let mut write_average_request_size = write_bytes / writes_completed;
                write_average_request_size = if write_average_request_size.is_nan() { 0. } else { write_average_request_size };
                let time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();

                println!("{:30} {:8} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         reads_completed,
                         writes_completed,
                         read_bytes / (1024.*1024.),
                         write_bytes / (1024.*1024.),
                         reads_merged,
                         writes_merged,
                         read_percentage_merged,
                         write_percentage_merged,
                         read_average_time_ms,
                         write_average_time_ms,
                         queue,
                         read_average_request_size / (1024.*1024.),
                         write_average_request_size / (1024.*1024.),
                );
            }
        }
    }
}

pub fn print_iostat_x_header()
{
    println!("{:30} {:8} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
             "time",
             "Device",
             "r/s",
             "w/s",
             "rMB/s",
             "wMB/s",
             "rrqm/s",
             "wrqm/s",
             "%rrqm/s",
             "%wrqm/s",
             "r_await",
             "w_await",
             "aqu-sz",
             "rareq-sz",
             "wareq-sz",
    );
}