//use std::collections::BTreeMap;
use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

//use crate::Statistic;
use crate::{CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, HistoricalData, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_RIGHT, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE, Statistic};

#[derive(Debug)]
pub struct YbIoDetails {
    pub glog_info_messages: f64,
    pub glog_warning_messages: f64,
    pub glog_error_messages: f64,
    pub log_bytes_logged: f64,
    pub log_reader_bytes_read: f64,
    pub log_sync_latency_count: f64,
    pub log_sync_latency_sum: f64,
    pub log_append_latency_count: f64,
    pub log_append_latency_sum: f64,
    pub log_cache_disk_reads: f64,
    pub rocksdb_flush_write_bytes: f64,
    pub intentsdb_rocksdb_flush_write_bytes: f64,
    pub rocksdb_compact_read_bytes: f64,
    pub intentsdb_rocksdb_compact_read_bytes: f64,
    pub rocksdb_compact_write_bytes: f64,
    pub intentsdb_rocksdb_compact_write_bytes: f64,
    pub rocksdb_write_raw_block_micros_count: f64,
    pub rocksdb_write_raw_block_micros_sum: f64,
    pub rocksdb_sst_read_micros_count: f64,
    pub rocksdb_sst_read_micros_sum: f64,
    pub intentsdb_rocksdb_block_cache_hit: f64,
    pub intentsdb_rocksdb_block_cache_miss: f64,
    pub rocksdb_block_cache_hit: f64,
    pub rocksdb_block_cache_miss: f64,
}

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
        // info messages are buffered and written when it's conveniently to do so.
        // This may lead to info and other messages not being strict ordered in time.
        // this also means info messages are not equivalent to an IO.
        // warning and error messages are stored when they occur, so these do count as write IOs.
        "glog_info_messages" |
        "glog_warning_message" |
        "glog_error_messages" => {
            let value = match sample.value
            {
                // Value::Untyped is the old YugabyteDB prometheus-metrics type
                Value::Untyped(value) => value,
                // Value::Counter is the new YugabyteDB prometheus-metrics type
                Value::Counter(value) => value,
                _ => {
                    panic!("{} value enum type should be Untyped or Counter!", sample.metric);
                },
            };
            //let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
            let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
            statistics
                .entry(( hostname.to_string(), sample.metric.clone(), metric_type.to_string(), "".to_string() ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    row.first_value = false;
                    debug!("{}: {} last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
                } )
                .or_insert(
                    Statistic
                    {
                        last_value: value,
                        last_timestamp: sample.timestamp,
                        first_value: true,
                        ..Default::default()
                    }
                );
        },
        "intentsdb_rocksdb_block_cache_hit" |
        "intentsdb_rocksdb_block_cache_miss" |
        "rocksdb_block_cache_hit" |
        "rocksdb_block_cache_miss" |
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
            let value = match sample.value
            {
                // Value::Untyped is the old YugabyteDB prometheus-metrics type
                Value::Untyped(value) => value,
                // Value::Counter is the new YugabyteDB prometheus-metrics type
                Value::Counter(value) => value,
                _ => {
                    panic!("{} value enum type should be Untyped or Counter!", sample.metric);
                },
            };
            //let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
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
                     (log_append_latency_sum / log_append_latency_count).max(0.) / 1000.,
                     (log_sync_latency_sum / log_sync_latency_count).max(0.) / 1000.,
                     (rocksdb_flush_write_bytes + intentsdb_rocksdb_flush_write_bytes) / (1024.*1024.),
                     (rocksdb_compact_read_bytes + intentsdb_rocksdb_compact_read_bytes) / (1024.*1024.),
                     (rocksdb_compact_write_bytes + intentsdb_rocksdb_compact_write_bytes) / (1024.*1024.),
                     rocksdb_write_raw_block_micros_count,
                     (rocksdb_write_raw_block_micros_sum / rocksdb_write_raw_block_micros_count).max(0.) / 1000.,
                     rocksdb_sst_read_micros_count,
                     (rocksdb_sst_read_micros_sum / rocksdb_sst_read_micros_count).max(0.) / 1000.,
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

pub fn create_yb_io_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.yb_io_details.keys().map(|(hostname, _)| hostname).unique()
    {
        let number_of_areas = 4;
        let y_size_of_root = 2800;
        let filename = format!("{}_yb_io.png", filter_hostname);
        let root = BitMapBackend::new(&filename, (1280, y_size_of_root)).into_drawing_area();
        let multiroot = root.split_evenly((number_of_areas, 1));

        let start_time = unlocked_historical_data.yb_io_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.yb_io_details
            .keys()
            .filter(|(hostname, _)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();
        let ((_, _), latest) = unlocked_historical_data.yb_io_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .max_by_key(|((timestamp, _), _)| timestamp)
            .unwrap();

        // MBPS
        let low_value_mbps = 0.;
        let high_value_mbps = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_bytes_logged + row.log_reader_bytes_read + row.rocksdb_flush_write_bytes + row.intentsdb_rocksdb_flush_write_bytes + row.rocksdb_compact_read_bytes + row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes) / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        multiroot[0].fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&multiroot[0])
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Yugabyte IO MBPS: {}", filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value_mbps..high_value_mbps)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("MBPS")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_log_reader_bytes_read = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_reader_bytes_read / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_reader_bytes_read = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_reader_bytes_read / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.log_reader_bytes_read + row.rocksdb_flush_write_bytes + row.intentsdb_rocksdb_flush_write_bytes + row.rocksdb_compact_read_bytes + row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "log_reader_bytes_read", min_log_reader_bytes_read, max_log_reader_bytes_read, latest.log_reader_bytes_read / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_rocksdb_flush_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_flush_write_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_flush_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_flush_write_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_flush_write_bytes + row.intentsdb_rocksdb_flush_write_bytes + row.rocksdb_compact_read_bytes + row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb_flush_write_bytes", min_rocksdb_flush_write_bytes, max_rocksdb_flush_write_bytes, latest.rocksdb_flush_write_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_intentsdb_rocksdb_flush_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_flush_write_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_intentsdb_rocksdb_flush_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_flush_write_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.intentsdb_rocksdb_flush_write_bytes + row.rocksdb_compact_read_bytes + row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "intentsdb_rocksdb_flush_write_bytes", min_intentsdb_rocksdb_flush_write_bytes, max_intentsdb_rocksdb_flush_write_bytes, latest.intentsdb_rocksdb_flush_write_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        let min_rocksdb_compact_read_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_compact_read_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_compact_read_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_compact_read_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_compact_read_bytes + row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb_compact_read_bytes", min_rocksdb_compact_read_bytes, max_rocksdb_compact_read_bytes, latest.rocksdb_compact_read_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
        let min_intentsdb_rocksdb_compact_read_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_compact_read_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_intentsdb_rocksdb_compact_read_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_compact_read_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.intentsdb_rocksdb_compact_read_bytes + row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(6))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "intentsdb_rocksdb_compact_read_bytes", min_intentsdb_rocksdb_compact_read_bytes, max_intentsdb_rocksdb_compact_read_bytes, latest.intentsdb_rocksdb_compact_read_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
        let min_rocksdb_compact_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_compact_write_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_compact_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_compact_write_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_compact_write_bytes + row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(7))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb_compact_write_bytes", min_rocksdb_compact_write_bytes, max_rocksdb_compact_write_bytes, latest.rocksdb_compact_write_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(7).filled()));
        let min_intentsdb_rocksdb_compact_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_compact_write_bytes / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_intentsdb_rocksdb_compact_write_bytes = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_compact_write_bytes / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.intentsdb_rocksdb_compact_write_bytes + row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(8))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "intentsdb_rocksdb_compact_write_bytes", min_intentsdb_rocksdb_compact_write_bytes, max_intentsdb_rocksdb_compact_write_bytes, latest.intentsdb_rocksdb_compact_write_bytes / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));
        let min_log_bytes_logged = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_bytes_logged / (1024. * 1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_bytes_logged = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_bytes_logged / (1024. * 1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.log_bytes_logged) / (1024.*1024.))),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "log_bytes_logged", min_log_bytes_logged, max_log_bytes_logged, latest.log_bytes_logged / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));

        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        // IOPS
        let low_value_iops = 0.;
        let high_value_iops = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_info_messages + row.glog_warning_messages + row.glog_error_messages + row.log_cache_disk_reads + row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        multiroot[1].fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&multiroot[1])
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Yugabyte IO IOPS: {}", filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value_iops..high_value_iops)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("IOPS")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_glog_info_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_info_messages)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_glog_info_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_info_messages)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.glog_info_messages + row.glog_warning_messages + row.glog_error_messages + row.log_cache_disk_reads + row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "glog_info_messages", min_glog_info_messages, max_glog_info_messages, latest.glog_info_messages))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_glog_warning_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_warning_messages)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_glog_warning_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_warning_messages)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.glog_warning_messages + row.glog_error_messages + row.log_cache_disk_reads + row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "glog_warning_messages", min_glog_warning_messages, max_glog_warning_messages, latest.glog_warning_messages))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_glog_error_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_error_messages)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_glog_error_messages = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.glog_error_messages)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.glog_error_messages + row.log_cache_disk_reads + row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "glog_error_messages", min_glog_error_messages, max_glog_error_messages, latest.glog_error_messages))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        let min_log_cache_disk_reads = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_cache_disk_reads)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_cache_disk_reads = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_cache_disk_reads)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.log_cache_disk_reads + row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "log_cache_disk_reads", min_log_cache_disk_reads, max_log_cache_disk_reads, latest.log_cache_disk_reads))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
        let min_log_append_latency_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_append_latency_count)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_append_latency_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.log_append_latency_count)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.log_append_latency_count + row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(6))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "log_append_latency_count", min_log_append_latency_count, max_log_append_latency_count, latest.log_append_latency_count))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
        let min_rocksdb_sst_read_micros_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_sst_read_micros_count)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_sst_read_micros_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_sst_read_micros_count)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.rocksdb_sst_read_micros_count + row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(7))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb_sst_read_micros_count", min_rocksdb_sst_read_micros_count, max_rocksdb_sst_read_micros_count, latest.rocksdb_sst_read_micros_count))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(7).filled()));
        let min_rocksdb_write_raw_blocks_micros_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_write_raw_block_micros_count)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_write_raw_blocks_micros_count = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_write_raw_block_micros_count)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.rocksdb_write_raw_block_micros_count)),
                                                0.0, Palette99::pick(8))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb_write_raw_blocks_micros_count", min_rocksdb_write_raw_blocks_micros_count, max_rocksdb_write_raw_blocks_micros_count, latest.rocksdb_write_raw_block_micros_count))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));

        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        // Latencies
        let low_value_latency = 0.;
        let mut max_latencies = vec![unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_sync_latency_sum / row.log_sync_latency_count).max(0.) / 1000.)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap()
        ];
        max_latencies.push(unlocked_historical_data.yb_io_details
                               .iter()
                               .filter(|((hostname, _), _)| hostname == filter_hostname)
                               .map(|((_, _), row)| (row.log_append_latency_sum / row.log_append_latency_count).max(0.) / 1000.)
                               .max_by(|a, b| a.partial_cmp(b).unwrap())
                               .unwrap()
        );
        max_latencies.push(unlocked_historical_data.yb_io_details
                               .iter()
                               .filter(|((hostname, _), _)| hostname == filter_hostname)
                               .map(|((_, _), row)| (row.rocksdb_sst_read_micros_sum / row.rocksdb_sst_read_micros_count).max(0.) / 1000.)
                               .max_by(|a, b| a.partial_cmp(b).unwrap())
                               .unwrap()
        );
        max_latencies.push(unlocked_historical_data.yb_io_details
                               .iter()
                               .filter(|((hostname, _), _)| hostname == filter_hostname)
                               .map(|((_, _), row)| (row.rocksdb_write_raw_block_micros_sum / row.rocksdb_write_raw_block_micros_count).max(0.) / 1000.)
                               .max_by(|a, b| a.partial_cmp(b).unwrap())
                               .unwrap()
        );
        let high_value_latency = max_latencies.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
        multiroot[2].fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&multiroot[2])
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Yugabyte latency (ms): {}", filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value_latency..*high_value_latency)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("latency (ms)")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_log_append_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_append_latency_sum / row.log_append_latency_count).max(0.) / 1000.)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_append_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_append_latency_sum / row.log_append_latency_count).max(0.) / 1000.)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.yb_io_details
                                                .iter()
                                                .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                .map(|((_, timestamp), row)| (*timestamp, (row.log_append_latency_sum / row.log_append_latency_count).max(0.) / 1000.)),
                                            Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:<40} min: {:10.2}, max: {:10.2}, latest:{:10.2}", "log_append_latency", min_log_append_latency, max_log_append_latency, ((latest.log_append_latency_sum / latest.log_append_latency_count).max(0.) / 1000.)))
            .legend(|(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        let min_log_sync_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_sync_latency_sum / row.log_sync_latency_count).max(0.) / 1000.)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_log_sync_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.log_sync_latency_sum / row.log_sync_latency_count).max(0.) / 1000.)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.yb_io_details
                                                    .iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.log_sync_latency_sum / row.log_sync_latency_count).max(0.) / 1000.)),
                                                Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:<40} min: {:10.2}, max: {:10.2}, latest:{:10.2}", "log_sync_latency", min_log_sync_latency, max_log_sync_latency, ((latest.log_sync_latency_sum / latest.log_sync_latency_count).max(0.) / 1000.)))
            .legend(|(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_rocksdb_sst_read_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.rocksdb_sst_read_micros_sum / row.rocksdb_sst_read_micros_count).max(0.) / 1000.)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_sst_read_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.rocksdb_sst_read_micros_sum / row.rocksdb_sst_read_micros_count).max(0.) / 1000.)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.yb_io_details
                                                    .iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_sst_read_micros_sum / row.rocksdb_sst_read_micros_count).max(0.) / 1000.)),
                                                Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:<40} min: {:10.2}, max: {:10.2}, latest:{:10.2}", "rocksdb_sst_read_micros", min_rocksdb_sst_read_latency, max_rocksdb_sst_read_latency, ((latest.rocksdb_sst_read_micros_sum / latest.rocksdb_sst_read_micros_count).max(0.) / 1000.)))
            .legend(|(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_rocksdb_write_raw_block_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.rocksdb_write_raw_block_micros_sum / row.rocksdb_write_raw_block_micros_count).max(0.) / 1000.)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_write_raw_block_latency = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.rocksdb_write_raw_block_micros_sum / row.rocksdb_write_raw_block_micros_count).max(0.) / 1000.)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(LineSeries::new(unlocked_historical_data.yb_io_details
                                                    .iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_write_raw_block_micros_sum / row.rocksdb_write_raw_block_micros_count).max(0.) / 1000.)),
                                                Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:<40} min: {:10.2}, max: {:10.2}, latest:{:10.2}", "rocksdb_write_raw_block_micros", min_rocksdb_write_raw_block_latency, max_rocksdb_write_raw_block_latency, ((latest.rocksdb_write_raw_block_micros_sum / latest.rocksdb_write_raw_block_micros_count).max(0.) / 1000.)))
            .legend(|(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));

        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();

        // Block cache hits
        let low_value_block_cache = 0.;
        let high_value_block_cache = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| (row.intentsdb_rocksdb_block_cache_hit + row.intentsdb_rocksdb_block_cache_miss + row.rocksdb_block_cache_hit + row.rocksdb_block_cache_miss))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        multiroot[3].fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&multiroot[3])
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Yugabyte block cache per second: {}", filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value_block_cache..high_value_block_cache)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("cache actions per second")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_intentsdb_rocksdb_block_cache_hit = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_block_cache_hit)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_intentsdb_rocksdb_block_cache_hit = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_block_cache_hit)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.intentsdb_rocksdb_block_cache_hit + row.intentsdb_rocksdb_block_cache_miss + row.rocksdb_block_cache_hit + row.rocksdb_block_cache_miss))),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "intentsdb rocksdb block cache hit", min_intentsdb_rocksdb_block_cache_hit, max_intentsdb_rocksdb_block_cache_hit, latest.intentsdb_rocksdb_block_cache_hit))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));

        let min_intentsdb_rocksdb_block_cache_miss = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_block_cache_miss)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_intentsdb_rocksdb_block_cache_miss = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.intentsdb_rocksdb_block_cache_miss)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, timestamp), row)| (*timestamp, (row.intentsdb_rocksdb_block_cache_miss + row.rocksdb_block_cache_hit + row.rocksdb_block_cache_miss))),
                 0.0, Palette99::pick(2))
        )
                                    .unwrap()
                                    .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "intentsdb rocksdb block cache miss", min_intentsdb_rocksdb_block_cache_miss, max_intentsdb_rocksdb_block_cache_miss, latest.intentsdb_rocksdb_block_cache_miss))
                                    .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
        let min_rocksdb_block_cache_hit = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_block_cache_hit)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_block_cache_hit = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_block_cache_hit)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_block_cache_hit + row.rocksdb_block_cache_miss))),
                 0.0, Palette99::pick(3))
        )
                                    .unwrap()
                                    .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb block cache hit", min_rocksdb_block_cache_hit, max_rocksdb_block_cache_hit, latest.rocksdb_block_cache_hit))
                                    .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));
        let min_rocksdb_block_cache_miss = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_block_cache_miss)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_rocksdb_block_cache_miss = unlocked_historical_data.yb_io_details
            .iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.rocksdb_block_cache_miss)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_io_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.rocksdb_block_cache_miss))),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:40} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "rocksdb block cache miss", min_rocksdb_block_cache_miss, max_rocksdb_block_cache_miss, latest.rocksdb_block_cache_miss))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(4).filled()));
        contextarea.configure_series_labels()
            .border_style(BLACK)
            .background_style(WHITE.mix(0.7))
            .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
            .position(UpperLeft)
            .draw()
            .unwrap();
    }
}
