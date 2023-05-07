use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData};

#[derive(Debug)]
pub struct NodeDiskDetails {
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
            row.first_value = false;
            debug!("{} device: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, device, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
        })
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
                first_value: true,
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
                    .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; row.first_value = false; })
                    .or_insert(Statistic { per_second_value, last_timestamp, first_value: true, ..Default::default() });
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
        if statistics.iter().find(|((host, metric, _, _), row)| host == hostname && metric == "node_disk_read_bytes_total" && row.first_value != true).is_some()
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
        if statistics.iter().find(|((host, metric, _, _), row)| host == hostname && metric == "node_xfs_read_calls_total" && row.first_value != true).is_some()
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
        if statistics.iter().find(|((host, metric, _, _), row)| host == hostname && metric == "node_disk_read_bytes_total" && row.first_value != true).is_some()
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
        if statistics.iter().find(|((host, metric, _, _), row)| host == hostname && metric == "node_disk_read_bytes_total" && row.first_value).is_some()
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

pub fn create_disk_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.disk_details.keys().map(|(hostname, _, _)| hostname).unique()
    {
        for current_device in unlocked_historical_data.disk_details.iter().filter(|((hostname, _, _), _)| hostname == filter_hostname).map(|((_, _, device), _)| device).unique()
        {
            // MBPS plot
            // set the plot specifics
            let start_time = unlocked_historical_data.disk_details
                .keys()
                .filter(|(hostname, _, device)| hostname == filter_hostname && device == current_device)
                .map(|(_, timestamp, _)| timestamp)
                .min()
                .unwrap();
            let end_time = unlocked_historical_data.disk_details
                .keys()
                .filter(|(hostname, _, device)| hostname == filter_hostname && device == current_device)
                .map(|(_, timestamp, _)| timestamp)
                .max()
                .unwrap();
            let low_value_mbps: f64 = 0.0;
            let high_value_mbps = unlocked_historical_data.disk_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| (row.reads_bytes_s + row.writes_bytes_s) / (1024.*1024.))
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let filename = format!("{}_disk_mbps_{}.png", filter_hostname, current_device);

            // create the plot
            let root = BitMapBackend::new(&filename, (1280, 900)).into_drawing_area();
            root.fill(&WHITE).unwrap();
            let mut contextarea = ChartBuilder::on(&root)
                .set_label_area_size(LabelAreaPosition::Left, 60)
                .set_label_area_size(LabelAreaPosition::Bottom, 50)
                .set_label_area_size(LabelAreaPosition::Right, 60)
                .caption(format!("Disk MBPS: {} {}", filter_hostname, current_device), ("monospace", 30))
                .build_cartesian_2d(*start_time..*end_time, low_value_mbps..high_value_mbps)
                .unwrap();
            contextarea.configure_mesh()
                .x_labels(4)
                .x_label_formatter(&|x| x.to_rfc3339().to_string())
                .y_desc("MBPS")
                .label_style(("monospace", 17))
                .draw()
                .unwrap();
            let min_reads_mbps = unlocked_historical_data.disk_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.reads_bytes_s / (1024.*1024.))
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_reads_mbps = unlocked_historical_data.disk_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.reads_bytes_s / (1024.*1024.))
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.disk_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, (row.reads_bytes_s + row.writes_bytes_s) / (1024.*1024.))),
                                                    0.0, Palette99::pick(1))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Read MBPS", min_reads_mbps, max_reads_mbps))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
            let min_writes_mbps = unlocked_historical_data.disk_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.writes_bytes_s / (1024.*1024.))
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_writes_mbps = unlocked_historical_data.disk_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.writes_bytes_s / (1024.*1024.))
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.disk_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, row.writes_bytes_s / (1024.*1024.))),
                                                    0.0, Palette99::pick(2))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Write MBPS", min_writes_mbps, max_writes_mbps))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
            /*
            let min_scheduler_runtime = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.schedstat_runtime)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_scheduler_runtime = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.schedstat_runtime)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.schedstat_runtime)),
                                                    0.0, Palette99::pick(2))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "scheduler runtime", min_scheduler_runtime, max_scheduler_runtime))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
            let min_guest_nice = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.guest_nice)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_guest_nice = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.guest_nice)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.guest_nice + row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(3))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "guest nice", min_guest_nice, max_guest_nice))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
            let min_guest_user = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.guest_user)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_guest_user = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.guest_user)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(4))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "guest user", min_guest_user, max_guest_user))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
            let min_softirq = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.softirq)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_softirq = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.softirq)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(5))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "softirq", min_softirq, max_softirq))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
            let min_irq = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.irq)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_irq = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.irq)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.irq + row.steal + row.iowait + row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(6))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "irq", min_irq, max_irq))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
            let min_iowait = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.iowait)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_iowait = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.iowait)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.iowait + row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(8))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "iowait", min_iowait, max_iowait))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));
            let min_system = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.system)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_system = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.system)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.system + row.nice + row.user)),
                                                    0.0, Palette99::pick(9))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "system", min_system, max_system))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(9).filled()));
            let min_nice = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.nice)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_nice = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.nice)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.nice + row.user)),
                                                    0.0, Palette99::pick(10))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "nice", min_nice, max_nice))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(10).filled()));
            let min_user = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.user)
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_user = unlocked_historical_data.cpu_details.iter()
                .filter(|((hostname, _), _)| hostname == filter_hostname)
                .map(|((_, _), row)| row.user)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, row.user)),
                                                    0.0, GREEN)
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "user", min_user, max_user))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], GREEN.filled()));
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.cpu_details.iter()
                                                        .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                        .map(|((_, timestamp), row)| (*timestamp, (row.idle + row.guest_nice + row.guest_user + row.softirq + row.irq + row.steal + row.iowait + row.system + row.nice + row.user).round())),
                                                    0.0, TRANSPARENT).border_style(RED)
            )
                .unwrap()
                .label("total cpu")
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], RED.filled()));

             */
            contextarea.configure_series_labels()
                .border_style(BLACK)
                .background_style(WHITE.mix(0.7))
                .label_font(("monospace", 15))
                .position(UpperLeft)
                .draw()
                .unwrap();
        }
    }
}
