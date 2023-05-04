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
    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
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
            debug!("{}: {} last_value: {}, last_timestamp: {}", hostname, sample.metric, row.last_value, row.last_timestamp);
        } )
        .or_insert(
            Statistic
            {
                last_value: value,
                last_timestamp: sample.timestamp,
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
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").count() > 0
        {
            let generic_heap_size = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let generic_allocated = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_current_allocated_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let tcmalloc_pageheap_free_bytes = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_free_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let tcmalloc_max_total_thread_cache_bytes = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_max_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let tcmalloc_current_total_thread_cache_bytes = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_current_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let tcmalloc_pageheap_unmapped_bytes = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_unmapped_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_call = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Call" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_read_buffer = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_compressed_read_buffer = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Compressed_Read_Buffer" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_tablets = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Tablets" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default(); // master does not have tablets
            let mem_tracker_log_cache = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_log_cache" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_blockbasedtable = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_BlockBasedTable" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_compressed_read_buffer_receive = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Compressed_Read_Buffer_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_read_buffer_inbound_rpc_sending = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Sending" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_inbound_rpc_receive = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_inbound_rpc_reading = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Reading" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // doesn't exist when no RPC calls have been made
            let mem_tracker_read_buffer_outbound_rpc_queueing = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Queueing" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_read_buffer_outbound_rpc_receive = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default(); // master follower does not have this
            let mem_tracker_read_buffer_outbound_rpc_sending = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Sending" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default(); // master follower does not have this
            let mem_tracker_read_buffer_outbound_rpc_reading = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Outbound_RPC_Reading" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default(); // master follower does not have this
            let mem_tracker_independent_allocations = mem_tracker_compressed_read_buffer_receive + mem_tracker_read_buffer_inbound_rpc_sending + mem_tracker_read_buffer_inbound_rpc_receive + mem_tracker_read_buffer_inbound_rpc_reading + mem_tracker_read_buffer_outbound_rpc_queueing + mem_tracker_read_buffer_outbound_rpc_receive + mem_tracker_read_buffer_outbound_rpc_sending + mem_tracker_read_buffer_outbound_rpc_reading;
            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
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