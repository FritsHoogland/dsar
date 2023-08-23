use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE};

#[derive(Debug)]
pub struct YbMemoryDetails {
    pub generic_heap: f64,
    pub generic_allocated: f64,
    pub tcmalloc_pageheap_free: f64,
    pub tcmalloc_max_total_thread_cache: f64,
    pub tcmalloc_current_total_thread_cache: f64,
    pub tcmalloc_pageheap_unmapped: f64,
    pub mem_tracker: f64,
    pub mem_tracker_call: f64,
    pub mem_tracker_read_buffer: f64,
    pub mem_tracker_compressed_read_buffer: f64,
    pub mem_tracker_tablets: f64,
    pub mem_tracker_log_cache: f64,
    pub mem_tracker_blockbasedtable: f64,
    pub mem_tracker_independent_allocs: f64
}

pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    let value = match sample.value
    {
        // Value::Untyped is the old YugabyteDB prometheus-metrics type
        Value::Untyped(value) => value,
        // Value::Gauge is the new YugabyteDB prometheus-metrics type
        Value::Gauge(value) => value,
        _ => {
            panic!("{} value enum type should be Untyped or Gauge!", sample.metric);
        },
    };
    //let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
    let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();

    statistics
        .entry((
            hostname.to_string(),
            sample.metric.clone(),
            metric_type.to_string(),
            "".to_string(),
        ))
        .and_modify( |row| {
            row.last_value = value;
            row.last_timestamp = sample.timestamp;
            row.first_value = false;
            debug!("{}: {} last_value: {}, last_timestamp: {}", hostname, sample.metric, row.last_value, row.last_timestamp);
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
}

pub fn print_yb_memory(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().any(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server" )
        {
            let generic_heap_size = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let generic_allocated = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_current_allocated_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let tcmalloc_pageheap_free_bytes = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_free_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let tcmalloc_max_total_thread_cache_bytes = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_max_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let tcmalloc_current_total_thread_cache_bytes = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_current_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let tcmalloc_pageheap_unmapped_bytes = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_unmapped_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_call = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Call" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_read_buffer = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_compressed_read_buffer = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Compressed_Read_Buffer" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_tablets = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Tablets" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // master does not have tablets
            let mem_tracker_log_cache = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_log_cache" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_blockbasedtable = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_BlockBasedTable" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_compressed_read_buffer_receive = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Compressed_Read_Buffer_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_read_buffer_inbound_rpc_sending = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Sending" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_inbound_rpc_receive = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_inbound_rpc_reading = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Reading" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_outbound_rpc_queueing = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Queueing" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
            let mem_tracker_read_buffer_outbound_rpc_receive = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // master follower does not have this
            let mem_tracker_read_buffer_outbound_rpc_sending = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Sending" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // master follower does not have this
            let mem_tracker_read_buffer_outbound_rpc_reading = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Reading" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // master follower does not have this
            let mem_tracker_independent_allocations = mem_tracker_compressed_read_buffer_receive + mem_tracker_read_buffer_inbound_rpc_sending + mem_tracker_read_buffer_inbound_rpc_receive + mem_tracker_read_buffer_inbound_rpc_reading + mem_tracker_read_buffer_outbound_rpc_queueing + mem_tracker_read_buffer_outbound_rpc_receive + mem_tracker_read_buffer_outbound_rpc_sending + mem_tracker_read_buffer_outbound_rpc_reading;
            let time = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            println!("{:50} {:8} {:10.0} {:10.0}|{:10.0} {:10.0} {:10.0} {:10.0}|{:10.0} {:10.0} {:10.0} {:10.0} {:10.0} {:10.0} {:10.0} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     generic_heap_size / (1024.*1024.),
                     generic_allocated / (1024.*1024.),
                     tcmalloc_pageheap_free_bytes / (1024.*1024.),
                     tcmalloc_max_total_thread_cache_bytes / (1024.*1024.),
                     tcmalloc_current_total_thread_cache_bytes / (1024.*1024.),
                     tcmalloc_pageheap_unmapped_bytes / (1024.*1024.),
                     mem_tracker / (1024.*1024.),
                     mem_tracker_call / (1024.*1024.),
                     mem_tracker_read_buffer / (1024.*1024.),
                     mem_tracker_compressed_read_buffer / (1024.*1024.),
                     mem_tracker_tablets / (1024.*1024.),
                     mem_tracker_log_cache / (1024.*1024.),
                     mem_tracker_blockbasedtable / (1024.*1024.),
                     mem_tracker_independent_allocations / (1024.*1024.),
            );
        }
    }
}

pub fn print_yb_memory_header()
{
    println!("{:50} {:8} {:>10} {:>10}|{:>10} {:>10} {:>10} {:>10}|{:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "",
             "",
             "",
             "generic",
             "",
             "",
             "",
             "tcmalloc",
             "memtrackers",
             "",
             "",
             "",
             "",
             "",
             "",
             "",
    );
    println!("{:50} {:8} {:>10} {:>10}|{:>10} {:>10} {:>10} {:>10}|{:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "total",
             "allocated",
             "ph_free",
             "tc_total",
             "tc_current",
             "ph_unmapd",
             "root",
             "call",
             "rd_buf",
             "c_rd_buf",
             "tablets",
             "log_cache",
             "bbt",
             "indep",
    );
}

pub fn create_yb_memory_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.yb_memory_details.keys().map(|(hostname, _)| hostname).unique()
    {
        let start_time = unlocked_historical_data.yb_memory_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .min()
            .unwrap();
        let end_time = unlocked_historical_data.yb_memory_details
            .keys()
            .filter(|(hostname,_)| hostname == filter_hostname)
            .map(|(_, timestamp)| timestamp)
            .max()
            .unwrap();
        let low_value: f64 = 0.0;
        let high_value = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _),_)| hostname == filter_hostname)
            .map(|((_, _), row)| row.generic_heap / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let ((_, _), latest) = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .max_by_key(|((timestamp, _), _)| timestamp)
            .unwrap();
        let filename = format!("{}_yb_memory.png", filter_hostname);

        // create the plot
        let root = BitMapBackend::new(&filename, (1280,900)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut contextarea = ChartBuilder::on(&root)
            .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
            .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
            .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
            .caption(format!("Yugabyte memory usage: {}",filter_hostname), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
            .build_cartesian_2d(*start_time..*end_time, low_value..high_value)
            .unwrap();
        contextarea.configure_mesh()
            .x_labels(4)
            .x_label_formatter(&|x| x.to_rfc3339())
            .y_desc("MB")
            .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
            .draw()
            .unwrap();
        let min_generic_heap = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.generic_heap / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_generic_heap = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.generic_heap / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.generic_heap / (1024.*1024.))),
                                                0.0, Palette99::pick(1))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "generic heap", min_generic_heap, max_generic_heap, latest.generic_heap / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
        let min_generic_allocated = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.generic_allocated / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_generic_allocated = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.generic_allocated / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.generic_allocated / (1024.*1024.))),
                                                0.0, Palette99::pick(2))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "generic allocated", min_generic_allocated, max_generic_allocated, latest.generic_allocated / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));


        let min_pageheap_free = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.tcmalloc_pageheap_free / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_pageheap_free = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.tcmalloc_pageheap_free / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.tcmalloc_pageheap_free + row.tcmalloc_current_total_thread_cache + row.mem_tracker_independent_allocs + row.mem_tracker) / (1024.*1024.))),
                                                0.0, Palette99::pick(13))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "pageheap free", min_pageheap_free, max_pageheap_free, latest.tcmalloc_pageheap_free / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(13).filled()));

        let min_tcmalloc_current_thread_cache = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.tcmalloc_current_total_thread_cache / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_tcmalloc_current_thread_cache = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.tcmalloc_current_total_thread_cache / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.tcmalloc_current_total_thread_cache + row.mem_tracker_independent_allocs + row.mem_tracker) / (1024.*1024.))),
                                                0.0, Palette99::pick(12))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "current total threadcache", min_tcmalloc_current_thread_cache, max_tcmalloc_current_thread_cache, latest.tcmalloc_current_total_thread_cache / (1024.* 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(12).filled()));
        let min_mem_tracker_independent = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_independent_allocs / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_independent = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_independent_allocs / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_independent_allocs + row.mem_tracker) / (1024.*1024.))),
                                                0.0, Palette99::pick(11))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker independent", min_mem_tracker_independent, max_mem_tracker_independent, latest.mem_tracker_independent_allocs / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(11).filled()));
        //
        // mem trackers
        // first the general mem trackers memory, and then the different area's
        //
        let min_mem_tracker = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.mem_tracker / (1024.*1024.))),
                                                0.0, Palette99::pick(3))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker", min_mem_tracker, max_mem_tracker, latest.mem_tracker / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(3).filled()));

        let min_mem_tracker_call = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_call / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_call = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_call / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_call + row.mem_tracker_read_buffer + row.mem_tracker_compressed_read_buffer + row.mem_tracker_tablets + row.mem_tracker_log_cache + row.mem_tracker_blockbasedtable) / (1024.*1024.))),
                                                0.0, Palette99::pick(9))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker call", min_mem_tracker_call, max_mem_tracker_call, latest.mem_tracker_call / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(9).filled()));
        let min_mem_tracker_read_buffer = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_read_buffer / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_read_buffer = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_read_buffer / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_read_buffer + row.mem_tracker_compressed_read_buffer + row.mem_tracker_tablets + row.mem_tracker_log_cache + row.mem_tracker_blockbasedtable) / (1024.*1024.))),
                                                0.0, Palette99::pick(8))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker read buffer", min_mem_tracker_read_buffer, max_mem_tracker_read_buffer, latest.mem_tracker_read_buffer / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(8).filled()));
        let min_mem_tracker_compressed_read_buffer = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_compressed_read_buffer / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_compressed_read_buffer = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_compressed_read_buffer / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_compressed_read_buffer + row.mem_tracker_tablets + row.mem_tracker_log_cache + row.mem_tracker_blockbasedtable) / (1024.*1024.))),
                                                0.0, Palette99::pick(7))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker comp. read buffer", min_mem_tracker_compressed_read_buffer, max_mem_tracker_compressed_read_buffer, latest.mem_tracker_compressed_read_buffer / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(7).filled()));
        let min_mem_tracker_tablets = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_tablets / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_tablets = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_tablets / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_tablets + row.mem_tracker_log_cache + row.mem_tracker_blockbasedtable) / (1024.*1024.))),
                                                0.0, Palette99::pick(6))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker tablets", min_mem_tracker_tablets, max_mem_tracker_tablets, latest.mem_tracker_tablets / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(6).filled()));
        let min_mem_tracker_log_cache = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_log_cache / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_log_cache = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_log_cache / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, (row.mem_tracker_log_cache + row.mem_tracker_blockbasedtable) / (1024.*1024.))),
                                                0.0, Palette99::pick(5))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker_log_cache", min_mem_tracker_log_cache, max_mem_tracker_log_cache, latest.mem_tracker_log_cache / (1024. * 1024.)))
            .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(5).filled()));
        let min_mem_tracker_blockbasedtable = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_blockbasedtable / (1024.*1024.))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max_mem_tracker_blockbasedtable = unlocked_historical_data.yb_memory_details.iter()
            .filter(|((hostname, _), _)| hostname == filter_hostname)
            .map(|((_, _), row)| row.mem_tracker_blockbasedtable / (1024.*1024.))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        contextarea.draw_series(AreaSeries::new(unlocked_historical_data.yb_memory_details.iter()
                                                    .filter(|((hostname, _), _)| hostname == filter_hostname)
                                                    .map(|((_, timestamp), row)| (*timestamp, row.mem_tracker_blockbasedtable / (1024.*1024.))),
                                                0.0, Palette99::pick(4))
        )
            .unwrap()
            .label(format!("{:30} min: {:10.2}, max: {:10.2}, latest: {:10.2}", "mem_tracker_blockbasedtable", min_mem_tracker_blockbasedtable, max_mem_tracker_blockbasedtable, latest.mem_tracker_blockbasedtable / (1024. * 1024.)))
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