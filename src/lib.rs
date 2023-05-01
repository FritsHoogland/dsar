use std::{collections, sync, time};
use prometheus_parse::{Scrape, Value};
use collections::HashMap;
use std::collections::BTreeMap;
use sync::mpsc::channel;
use time::Duration;
use log::*;
use rayon;
use chrono::{DateTime, Utc};
use itertools::Itertools;

#[derive(Debug, Default)]
pub struct Statistic {
    pub last_value: f64,
    pub delta_value: f64,
    pub per_second_value: f64,
    pub last_timestamp: DateTime<Utc>,
}

/*
#[derive(Debug)]
pub struct CpuDetails {
    hostname: String,
    timestamp: DateTime<Utc>,
    user: f64,
    system: f64,
    iowait: f64,
    nice: f64,
    irq: f64,
    softirq: f64,
    steal: f64,
    idle: f64,
    scheduler_runtime: f64,
    scheduler_wait: f64,
}

 */

pub async fn read_node_exporter_into_map(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize,
) -> HashMap<String, Scrape>
{
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let endpoints = vec!["metrics", "prometheus-metrics"];
                for endpoint in endpoints {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let node_exporter_values = read_node_exporter(&host, &port, &endpoint);
                        tx.send((format!("{}:{}:{}", host, port, endpoint), node_exporter_values)).expect("error sending data via tx (node_exporter)");
                    });
                }
            }
        }
    });
    let mut map_exporter_values: HashMap<String, Scrape> = HashMap::new();
    for (hostname_port, node_exporter_values) in rx {
        map_exporter_values.insert( hostname_port, node_exporter_values);
    }
    map_exporter_values
}

pub fn read_node_exporter(
    host: &str,
    port: &str,
    path: &str,
) -> Scrape
{
    let response = if let Ok(data_from_http) = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_millis(200))
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .get(format!("http://{}:{}/{}", host, port, path))
        .send()
    {
        if ! &data_from_http.status().is_success()
        {
            debug!("Non success response: {}:{}/{} = {}", host, port, path, &data_from_http.status());
        }
        else
        {
            debug!("Success response: {}:{}/{} = {}", host, port, path, &data_from_http.status());
        }
        data_from_http.text().unwrap()
    }
    else
    {
        debug!("Non-Ok success response: {}:{}/{}", host, port, path);
        String::new()
    };
    parse_node_exporter(response)
}

fn parse_node_exporter(
    node_exporter_data: String
) -> Scrape
{
    let lines : Vec<_> = node_exporter_data.lines().map(|lines| Ok(lines.to_owned())).collect();
    Scrape::parse(lines.into_iter()).unwrap()
}

pub async fn process_statistics(
    node_exporter_values: &HashMap<String, Scrape>,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    for (hostname, scrape) in node_exporter_values
    {
        for sample in &scrape.samples
        {
            match sample.metric.as_str()
            {
                // simple gauge values
                "node_procs_running" | "node_procs_blocked" | "node_load1" | "node_load5" | "node_load15" |
                "node_memory_Active_anon_bytes" |
                "node_memory_Active_bytes" |
                "node_memory_Active_file_bytes" |
                "node_memory_AnonHugePages_bytes" |
                "node_memory_AnonPages_bytes" |
                "node_memory_Bounce_bytes" |
                "node_memory_Buffers_bytes" |
                "node_memory_Cached_bytes" |
                "node_memory_CommitLimit_bytes" |
                "node_memory_Committed_AS_bytes" |
                "node_memory_DirectMap2M_bytes" |
                "node_memory_DirectMap4k_bytes" |
                "node_memory_Dirty_bytes" |
                "node_memory_FileHugePages_bytes" |
                "node_memory_FilePmdMapped_bytes" |
                "node_memory_HardwareCorrupted_bytes" |
                "node_memory_HugePages_Free" |
                "node_memory_HugePages_Rsvd" |
                "node_memory_HugePages_Surp" |
                "node_memory_HugePages_Total" |
                "node_memory_Hugepagesize_bytes" |
                "node_memory_Hugetlb_bytes" |
                "node_memory_Inactive_anon_bytes" |
                "node_memory_Inactive_bytes" |
                "node_memory_Inactive_file_bytes" |
                "node_memory_KReclaimable_bytes" |
                "node_memory_KernelStack_bytes" |
                "node_memory_Mapped_bytes" |
                "node_memory_MemAvailable_bytes" |
                "node_memory_MemFree_bytes" |
                "node_memory_MemTotal_bytes" |
                "node_memory_Mlocked_bytes" |
                "node_memory_NFS_Unstable_bytes" |
                "node_memory_PageTables_bytes" |
                "node_memory_Percpu_bytes" |
                "node_memory_SReclaimable_bytes" |
                "node_memory_SUnreclaim_bytes" |
                "node_memory_ShmemHugePages_bytes" |
                "node_memory_ShmemPmdMapped_bytes" |
                "node_memory_Shmem_bytes" |
                "node_memory_Slab_bytes" |
                "node_memory_SwapCached_bytes" |
                "node_memory_SwapFree_bytes" |
                "node_memory_SwapTotal_bytes" |
                "node_memory_Unevictable_bytes" |
                "node_memory_VmallocChunk_bytes" |
                "node_memory_VmallocTotal_bytes" |
                "node_memory_VmallocUsed_bytes" |
                "node_memory_WritebackTmp_bytes" |
                "node_memory_Writeback_bytes" => {
                    let Value::Gauge(value) = sample.value else { panic!("{} value enum type should be Gauge!", sample.metric)};
                    statistics
                    .entry((
                        hostname.clone(),
                        sample.metric.clone(),
                        "".to_string(),
                        "".to_string(),
                    ))
                    .and_modify( |row| {
                        row.last_value = value;
                        row.last_timestamp = sample.timestamp;
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
                // simple counter values
                "node_intr_total" | "node_context_switches_total" |
                "node_softnet_dropped_total" | "node_softnet_processed_total" | "node_softnet_times_squeezed_total" => {
                    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
                    statistics
                        .entry((
                            hostname.clone(),
                            sample.metric.clone(),
                            "".to_string(),
                            "".to_string(),
                        ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
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
                // counter values that are untyped
                "node_vmstat_oom_kill" |
                "node_vmstat_pgfault" |
                "node_vmstat_pgmajfault" |
                "node_vmstat_pgpgin" |
                "node_vmstat_pgpgout" |
                "node_vmstat_pswpin" |
                "node_vmstat_pswpout" => {
                    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
                    statistics
                        .entry((
                            hostname.clone(),
                            sample.metric.clone(),
                            "".to_string(),
                            "".to_string(),
                        ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
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
                "node_schedstat_running_seconds_total" | "node_schedstat_waiting_seconds_total" => {
                    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
                    let cpu_number = sample.labels.iter().find(|(label, _)| *label == "cpu").map(|(_, value)| value).unwrap();
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), cpu_number.to_string(), "".to_string() ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
                            debug!("{} cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
                "node_cpu_seconds_total" | "node_cpu_guest_seconds_total" => {
                    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
                    let cpu_number = sample.labels.iter().find(|(label, _)| *label == "cpu").map(|(_, value)| value).unwrap();
                    let mode = sample.labels.iter().find(|(label, _)| *label == "mode").map(|(_, value)| value).unwrap();
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), cpu_number.to_string(), mode.to_string() ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
                            debug!("{} mode: {}, cpu: {}: last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, mode, cpu_number, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
                "node_disk_read_bytes_total" | "node_disk_read_time_seconds_total" | "node_disk_reads_completed_total" | "node_disk_reads_merged_total" |
                "node_disk_written_bytes_total" | "node_disk_write_time_seconds_total" | "node_disk_writes_completed_total" | "node_disk_writes_merged_total" |
                "node_disk_discarded_sectors_total" | "node_disk_discard_time_seconds_total" | "node_disk_discards_completed_total" | "node_disk_discards_merged_total" |
                "node_disk_io_time_seconds_total" | "node_disk_io_time_weighted_seconds_total" => {
                    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
                    let device = sample.labels.iter().find(|(label, _)| *label == "device").map(|(_, value)| value).unwrap();
                    // do not store device mapper disk statistics
                    if device.starts_with("dm-") { continue };
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), device.to_string(), "".to_string() ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
                            debug!("{} device: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, device, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
                "node_network_receive_packets_total" | "node_network_transmit_packets_total" | "node_network_receive_bytes_total" | "node_network_transmit_bytes_total" |
                "node_network_receive_compressed_total" | "node_network_transmit_compressed_total" | "node_network_receive_multicast_total" |
                "node_network_receive_errs_total" | "node_network_transmit_errs_total" | "node_network_transmit_colls_total" | "node_network_receive_drop_total" | "node_network_transmit_drop_total" |
                "node_network_transmit_carrier_total" | "node_network_receive_fifo_total" | "node_network_transmit_fifo_total" => {
                    let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
                    let device = sample.labels.iter().find(|(label, _)| *label == "device").map(|(_, value)| value).unwrap();
                    if device.eq("lo") { continue };
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), device.to_string(), "".to_string() ))
                        .and_modify( |row| {
                            row.delta_value = value - row.last_value;
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
                            debug!("{} device: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, device, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
                "node_sockstat_sockets_used" | "node_sockstat_TCP_inuse" | "node_sockstat_UDP_inuse" | "node_sockstat_RAW_inuse" | "node_sockstat_FRAG_inuse" | "node_sockstat_TCP_tw" |
                "node_sockstat_TCP6_inuse" | "node_sockstat_UDP6_inuse" | "node_sockstat_RAW6_inuse" | "node_sockstat_FRAG6_inuse" => {
                    let Value::Gauge(value) = sample.value else { panic!("{} value enum type should be Gauge!", sample.metric)};
                    statistics
                        .entry((
                            hostname.clone(),
                            sample.metric.clone(),
                            "".to_string(),
                            "".to_string(),
                        ))
                        .and_modify( |row| {
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
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
                // YB statistics. These are untyped, but are counters.
                "cpu_stime" | "cpu_utime" | "tcp_bytes_received" | "tcp_bytes_sent" => {
                    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
                    let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), metric_type.to_string(), "".to_string() ))
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
                }
                // YB statistics. These are untyped, but are gauge values.
                "generic_heap_size" | "generic_current_allocated_bytes" |
                "tcmalloc_pageheap_free_bytes" | "tcmalloc_max_total_thread_cache_bytes" | "tcmalloc_current_total_thread_cache_bytes" | "tcmalloc_pageheap_unmapped_bytes" |
                "mem_tracker" |
                "mem_tracker_Call" | "mem_tracker_Call_Outbound_RPC" | "mem_tracker_Call_Inbound_RPC" | "mem_tracker_Call_Redis" | "mem_tracker_Call_CQL" |
                "mem_tracker_Read_Buffer" |
                "mem_tracker_Read_Buffer_Inbound_RPC" | "mem_tracker_Read_Buffer_Inbound_RPC_Sending" | "mem_tracker_Read_Buffer_Inbound_RPC_Receive" | "mem_tracker_Read_Buffer_Inbound_RPC_Reading" |
                "mem_tracker_Read_Buffer_Outbound_RPC" | "mem_tracker_Read_Buffer_Outbound_RPC_Queueing" | "mem_tracker_Read_Buffer_Outbound_RPC_Receive" | "mem_tracker_Read_Buffer_Outbound_RPC_Sending" | "mem_tracker_Read_Buffer_Outbound_RPC_Reading" |
                "mem_tracker_Read_Buffer_Redis" | "mem_tracker_Read_Buffer_Redis_Allocated" | "mem_tracker_Read_Buffer_Redis_Used" | "mem_tracker_Read_Buffer_Redis_Mandatory" |
                "mem_tracker_Read_Buffer_CQL" |
                "mem_tracker_Compressed_Read_Buffer" | "mem_tracker_Compressed_Read_Buffer_Receive" |
                "mem_tracker_BlockBasedTable" | "mem_tracker_BlockBasedTable_IntentsDB" | "mem_tracker_BlockBasedTable_RegularDB" |
                "mem_tracker_log_cache" |
                "mem_tracker_Tablets" | "mem_tracker_Tablets_transactions" => {
                    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
                    let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();

                    statistics
                        .entry((
                            hostname.clone(),
                            sample.metric.clone(),
                            metric_type.to_string(),
                            "".to_string(),
                        ))
                        .and_modify( |row| {
                            row.last_value = value;
                            row.last_timestamp = sample.timestamp;
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
                // YB IO related statistics. These are untyped, but are counters.
                // these are server level statistics
                "glog_info_messages" |
                "glog_warning_message" |
                "glog_error_messages" =>
                {
                    let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
                    let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
                    statistics
                        .entry(( hostname.clone(), sample.metric.clone(), metric_type.to_string(), "".to_string() ))
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
                }
                // YB IO related statistics. These are untyped, but are counters.
                // these are tablet level statistics
                "log_bytes_logged" |
                "log_reader_bytes_read" |
                "log_sync_latency_count" |
                "log_sync_latency_sum" |
                "log_append_latency_count" |
                "log_append_latency_sum" |
                "log_cache_disk_reads" |
                "rocksdb_flush_write_bytes" |
                "rocksdb_compact_read_bytes" |
                "rocksdb_compact_write_bytes" |
                "rocksdb_write_raw_block_micros_count" |
                "rocksdb_write_raw_block_micros_sum" |
                "rocksdb_sst_read_micros_count" |
                "rocksdb_sst_read_micros_sum" =>
                    {
                        let Value::Untyped(value) = sample.value else { panic!("{} value enum type should be Untyped!", sample.metric)};
                        let metric_type = sample.labels.iter().find(|(label, _)| *label == "metric_type").map(|(_, value)| value).unwrap();
                        let table_id = sample.labels.iter().find(|(label, _)| *label == "table_id").map(|(_, value)| value).unwrap();
                        statistics
                            .entry(( hostname.clone(), sample.metric.clone(), metric_type.to_string(), table_id.to_string() ))
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
                    }
                &_ => {},
            }
        }

        for (hostname, _) in node_exporter_values
        {
            // node_schedstat_running_seconds_total && node_schedstat_waiting_seconds_total
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_schedstat_running_seconds_total").count() > 0
            {
                for metric_name in vec!["node_schedstat_running_seconds_total", "node_schedstat_waiting_seconds_total"]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, cpu , _), _)| host == hostname && metric == metric_name && cpu != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    let last_timestamp = statistics.iter().filter(|((host, metric, cpu , _), _)| host == hostname && metric == metric_name && cpu != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; } )
                        .or_insert( Statistic { per_second_value:  per_second_value, last_timestamp: last_timestamp, ..Default::default() });
                }
            };
            // node_cpu_seconds_total
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_seconds_total").count() > 0
            {
                for mode in vec!["idle", "iowait", "irq", "nice", "softirq", "steal", "system", "user"]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    let last_timestamp = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                    statistics.entry( (hostname.to_string(), "node_cpu_seconds_total".to_string(), "total".to_string(), mode.to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; } )
                        .or_insert( Statistic { per_second_value:  per_second_value, last_timestamp: last_timestamp, ..Default::default() });
                }
            };
            // node_cpu_guest_seconds_total
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_guest_seconds_total").count() > 0
            {
                for mode in vec!["nice", "user" ]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    let last_timestamp = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                    statistics.entry( (hostname.to_string(), "node_cpu_guest_seconds_total".to_string(), "total".to_string(), mode.to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; } )
                        .or_insert( Statistic { per_second_value:  per_second_value, last_timestamp: last_timestamp, ..Default::default() });
                }
            };
            // disk IO
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_io_time_seconds_total").count() > 0
            {
                for metric_name in vec!["node_disk_read_bytes_total", "node_disk_read_time_seconds_total", "node_disk_reads_completed_total", "node_disk_reads_merged_total",
                                             "node_disk_written_bytes_total", "node_disk_write_time_seconds_total", "node_disk_writes_completed_total", "node_disk_writes_merged_total",
                                             "node_disk_discarded_sectors_total", "node_disk_discard_time_seconds_total", "node_disk_discards_completed_total", "node_disk_discards_merged_total",
                                             "node_disk_io_time_seconds_total", "node_disk_io_time_weighted_seconds_total"]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == metric_name && device != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    let last_timestamp = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == metric_name && device != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; } )
                        .or_insert( Statistic { per_second_value:  per_second_value, last_timestamp: last_timestamp, ..Default::default() });
                }
            };
            // network IO
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_packets_total").count() > 0
            {
                for metric_name in vec!["node_network_receive_packets_total", "node_network_transmit_packets_total", "node_network_receive_bytes_total", "node_network_transmit_bytes_total",
                                             "node_network_receive_compressed_total", "node_network_transmit_compressed_total", "node_network_receive_multicast_total",
                                             "node_network_receive_errs_total", "node_network_transmit_errs_total", "node_network_transmit_colls_total", "node_network_receive_drop_total", "node_network_transmit_drop_total",
                                             "node_network_transmit_carrier_total", "node_network_receive_fifo_total", "node_network_transmit_fifo_total"]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == metric_name && device != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    let last_timestamp = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == metric_name && device != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value; row.last_timestamp = last_timestamp; } )
                        .or_insert( Statistic { per_second_value:  per_second_value, last_timestamp: last_timestamp, ..Default::default() });
                }
            };
        }
    }
}

pub fn print_sar_s(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").count() > 0
        {
            let swap_free = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_used = swap_total - swap_free;
            let swap_used_percent = swap_used / swap_total * 100.;
            let swap_cached = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapCached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let swap_cached_percent = swap_cached / swap_used * 100.;
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.2} {:10.0} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     swap_free / (1024. * 1024.),
                     swap_used / (1024. * 1024.),
                     swap_used_percent,
                     swap_cached / (1024. * 1024.),
                     if swap_cached_percent.is_nan() { 0. } else { swap_cached_percent },
            );
        }
    }
}

pub fn print_sar_s_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "mbspwfree",
             "mbswpused",
             "%swpused",
             "mbswpcad",
             "%swpcad",
    );
}

pub fn print_sar_w(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").count() > 0
        {
            let pages_swap_in = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let pages_swap_out = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpout").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pswpin").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     pages_swap_in,
                     pages_swap_out,
            );
        }
    }
}

pub fn print_sar_w_header()
{
    println!("{:30} {:8} {:>10} {:>10}",
             "hostname",
             "time",
             "pswpin/s",
             "pswpout/s",
    );
}

pub fn print_sar_q(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").count() > 0
        {
            let runqueue_size = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "runqueue_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let tasklist_size = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "tasklist_size_does_not_exist").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let node_load_1 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_load_5 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load5").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_load_15 = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load15").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let node_procs_blocked = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_procs_blocked").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.2} {:10.2} {:10.2} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     runqueue_size,
                     tasklist_size,
                     node_load_1,
                     node_load_5,
                     node_load_15,
                     node_procs_blocked,
            );
        }
    }
}

pub fn print_sar_q_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "runq-sz",
             "plist-sz",
             "ldavg-1",
             "ldavg-5",
             "ldavg-15",
             "blocked",
    );
}

pub fn print_yb_cpu(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").count() > 0
        {
            let cpu_user = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let cpu_system = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_stime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let cpu_total = cpu_user + cpu_system;
            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "cpu_utime" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:50} {:8} {:10.3} {:10.3} {:10.3}",
                     hostname,
                     time.format("%H:%M:%S"),
                     cpu_user / 1000.,
                     cpu_system / 1000.,
                     cpu_total / 1000.,
            );
        }
    }
}

pub fn print_yb_cpu_header()
{
    println!("{:50} {:8} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "user/s",
             "sys/s",
             "tot/s",
    );
}
pub fn print_yb_network(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").count() > 0
        {
            let network_bytes_received = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let network_bytes_sent = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_sent" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let network_bytes_total = network_bytes_received + network_bytes_sent;
            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcp_bytes_received" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:50} {:8} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     network_bytes_received / (1024.*1024.),
                     network_bytes_sent / (1024.*1024.),
                     network_bytes_total / (1024.*1024.),
            );
        }
    }
}

pub fn print_yb_network_header()
{
    println!("{:50} {:8} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "rxMB/s",
             "txMB/s",
             "totMB/s",
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
            let mem_tracker_read_buffer_inbound_rpc_sending = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Sending" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_read_buffer_inbound_rpc_receive = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Receive" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let mem_tracker_read_buffer_inbound_rpc_reading = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "mem_tracker_Read_Buffer_Inbound_RPC_Reading" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
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

pub fn print_yb_io(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").count() > 0
        {
            let info_messages = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let warning_messages = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_warning_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap_or_default();
            let error_messages = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_error_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let log_bytes_logged: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_bytes_logged" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_reader_bytes_read: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_reader_bytes_read" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_flush_write_bytes: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_flush_write_bytes" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_compact_read_bytes: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_compact_read_bytes" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_compact_write_bytes: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_compact_write_bytes" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_cache_disk_reads: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_cache_disk_reads" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_sync_latency_count: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_sync_latency_count" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_sync_latency_sum: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_sync_latency_sum" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_append_latency_count: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_append_latency_count" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let log_append_latency_sum: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "log_append_latency_sum" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_write_raw_block_micros_count: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_write_raw_block_micros_count" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_write_raw_block_micros_sum: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_write_raw_block_micros_sum" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_sst_read_micros_count: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_sst_read_micros_count" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let rocksdb_sst_read_micros_sum: f64 = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "rocksdb_sst_read_micros_sum" && metric_type == "tablet").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();

            let time = statistics.iter().filter(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:50} {:8} {:10.2} {:10.2}|{:10.2} {:10.2} {:10.2} {:10.2} {:10.2} {:10.2}|{:10.0}|{:10.0} {:10.0}|{:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
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
                     rocksdb_flush_write_bytes / (1024.*1024.),
                     rocksdb_compact_read_bytes / (1024.*1024.),
                     rocksdb_compact_write_bytes / (1024.*1024.),
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

pub fn print_sar_b(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").count() > 0
        {
            let pages_paged_in = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let pages_paged_out = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgout").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let major_faults = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgfault").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_vmstat_pgpgin").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     pages_paged_in,
                     pages_paged_out,
                     faults,
                     major_faults,
            );
        }
    }
}

pub fn print_sar_b_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "pgpgin/s",
             "pgpgout/s",
             "fault/s",
             "majflt/s",
    );
}
pub fn print_sar_r(
    mode: &str,
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").count() > 0
        {
            let memory_free = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_available = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemAvailable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_used = memory_total - memory_free;
            let memory_buffers = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Buffers_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_cached = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Cached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_commit = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Committed_AS_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_swap_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_active = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_inactive = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_dirty = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Dirty_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_anonymous = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_AnonPages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_slab = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Slab_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_kernel_stack = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_KernelStack_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_pagetables = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_PageTables_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let memory_virtual_memory = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            if mode == "normal"
            {
                println!("{:30} {:8} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0}",
                         hostname,
                         time.format("%H:%M:%S"),
                         memory_free / (1024. * 1024.),
                         memory_available / (1024. * 1024.),
                         memory_used / (1024. * 1024.),
                         memory_used / memory_total * 100.,
                         memory_buffers / (1024. * 1024.),
                         memory_cached / (1024. * 1024.),
                         memory_commit / (1024. * 1024.),
                         memory_commit / (memory_total + memory_swap_total) * 100.,
                         memory_active / (1024. * 1024.),
                         memory_inactive / (1024. * 1024.),
                         memory_dirty / (1024. * 1024.)
                );
            } else {
                println!("{:30} {:8} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0}",
                         hostname,
                         time.format("%H:%M:%S"),
                         memory_free / (1024. * 1024.),
                         memory_available / (1024. * 1024.),
                         memory_used / (1024. * 1024.),
                         memory_used / memory_total * 100.,
                         memory_buffers / (1024. * 1024.),
                         memory_cached / (1024. * 1024.),
                         memory_commit / (1024. * 1024.),
                         memory_commit / (memory_total + memory_swap_total) * 100.,
                         memory_active / (1024. * 1024.),
                         memory_inactive / (1024. * 1024.),
                         memory_dirty / (1024. * 1024.),
                         memory_anonymous / (1024. * 1024.),
                         memory_slab / (1024. * 1024.),
                         memory_kernel_stack / (1024. * 1024.),
                         memory_pagetables / (1024. * 1024.),
                         memory_virtual_memory / (1024. * 1024.)
                );
            }
        }
    }
}

pub fn print_sar_r_header(
    mode: &str,
)
{
    if mode == "normal"
    {
        println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
                 "time",
                 "mbmemfree",
                 "mbavail",
                 "mbmemused",
                 "%memused",
                 "mbbuffers",
                 "mbcached",
                 "mbcommit",
                 "%commit",
                 "mbactive",
                 "mbinact",
                 "mbdirty"
        );
    }
    else
    {
        println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
                 "time",
                 "mbmemfree",
                 "mbavail",
                 "mbmemused",
                 "%memused",
                 "mbbuffers",
                 "mbcached",
                 "mbcommit",
                 "%commit",
                 "mbactive",
                 "mbinact",
                 "mbdirty",
                 "mbanonpg",
                 "mbslab",
                 "mbstack",
                 "mbpgtbl",
                 "mbvmused"
        );
    }
}

pub fn print_sar_n_edev(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_errs_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_errs_total").map(|((_, _, device, _), _)| device)
            {
                let receive_errors = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_errs_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_errors = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_errs_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_collisions = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_colls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let receive_drop = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_drop_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_drop = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_drop_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_carrier = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_carrier_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let receive_fifo = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_fifo_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_fifo = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_fifo_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_errs_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                println!("{:30} {:8} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         receive_errors,
                         transmit_errors,
                         transmit_collisions,
                         receive_drop,
                         transmit_drop,
                         transmit_carrier,
                         receive_fifo,
                         transmit_fifo
                );
            }
        }
    }
}

pub fn print_sar_n_edev_header()
{
    println!("{:30} {:8} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
             "time",
             "IFACE",
             "rxerr/s",
             "txerr/s",
             "coll/s",
             "rxdrop/s",
             "txdrop/s",
             "txcarr/s",
             "rxfifo/s",
             "txfifo/s"
    );
}

pub fn print_sar_n_dev(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_packets_total").count() > 0
        {
            for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_packets_total").map(|((_, _, device, _), _)| device)
            {
                let receive_packets = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_packets = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let receive_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let transmit_bytes = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let compressed_packets_received = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_compressed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let compressed_packets_transmit = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_compressed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let multicast_packets_received = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_multicast_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
                let time = statistics.iter().filter(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
                println!("{:30} {:8} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         time.format("%H:%M:%S"),
                         current_device,
                         receive_packets,
                         transmit_packets,
                         receive_bytes / (1024. * 1024.),
                         transmit_bytes / (1024. * 1024.),
                         compressed_packets_received,
                         compressed_packets_transmit,
                         multicast_packets_received
                );
            }
        }
    }
}

pub fn print_sar_n_dev_header()
{
    println!("{:30} {:8} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
             "time",
             "IFACE",
             "rxpck/s",
             "txpck/s",
             "rxMB/s",
             "txMB/s",
             "rxcmp/s",
             "txcmp/s",
             "rxmcst/s"
    );
}

pub fn print_sar_n_sock(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_sockets_used").count() > 0
        {
            let sockets_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_sockets_used").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets_tcp = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_TCP_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets_udp = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_UDP_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets_raw = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_RAW_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets_frag = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_FRAG_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets_timedwait = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_TCP_tw").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_sockets_used").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap_or_default();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.0} {:10.0} {:10.0} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     sockets_total,
                     sockets_tcp,
                     sockets_udp,
                     sockets_raw,
                     sockets_frag,
                     sockets_timedwait,
            );
        }
    }
}

pub fn print_sar_n_sock_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "totsck",
             "tcpsck",
             "udpsck",
             "rawsck",
             "ip-frag",
             "tcp-tw",
    );
}

pub fn print_sar_n_sock6(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_TCP6_inuse").count() > 0
        {
            let sockets6_tcp = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_TCP6_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets6_udp= statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_UDP6_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets6_raw= statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_RAW6_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let sockets6_frag= statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_FRAG6_inuse").map(|((_, _, _, _), statistic)| statistic.last_value).next().unwrap_or_default();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_sockstat_TCP6_inuse").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap_or_default();
            println!("{:30} {:8} {:10.0} {:10.0} {:10.0} {:10.0}",
                     hostname,
                     time.format("%H:%M:%S"),
                     sockets6_tcp,
                     sockets6_udp,
                     sockets6_raw,
                     sockets6_frag,
            );
        }
    }
}

pub fn print_sar_n_sock6_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "tcp6sck",
             "udp6sck",
             "raw6sck",
             "ip6-frag",
    );
}

pub fn print_sar_n_soft(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_processed_total").count() > 0
        {
            let soft_total = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_processed_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let soft_dropped = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_dropped_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let soft_squeezed = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_times_squeezed_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let soft_interproc_intr= statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_interpoc_intr_doesnotexist").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap_or_default();
            let soft_flow_limit = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_flow_limit_doesnotexist").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap_or_default();
            let time = statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_softnet_processed_total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).next().unwrap();
            println!("{:30} {:8} {:10.2} {:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     time.format("%H:%M:%S"),
                     soft_total,
                     soft_dropped,
                     soft_squeezed,
                     soft_interproc_intr,
                     soft_flow_limit,
            );
        }
    }
}

pub fn print_sar_n_soft_header()
{
    println!("{:30} {:8} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
             "time",
             "total/s",
             "dropd/s",
             "squeezd/s",
             "rx_rps/s",
             "flw_lim/s",
    );
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
             "await"
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
             "MB_wrtn"
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
                let mut read_average_wait = read_time / reads_completed;
                read_average_wait = if read_average_wait.is_nan() { 0. } else { read_average_wait };
                let mut write_average_wait = write_time / writes_completed;
                write_average_wait = if write_average_wait.is_nan() { 0. } else { write_average_wait };
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
                         read_bytes,
                         write_bytes,
                         reads_merged,
                         writes_merged,
                         read_percentage_merged,
                         write_percentage_merged,
                         read_average_wait,
                         write_average_wait,
                         queue,
                         read_average_request_size,
                         write_average_request_size,
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

pub fn print_sar_u(
    mode: &str,
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_seconds_total").count() > 0
        {
            let user_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let system_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "system" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let iowait_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "iowait" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let irq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "irq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let nice_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let softirq_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "softirq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let steal_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "steal" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let idle_time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "idle" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let guest_user = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let guest_nice = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let schedstat_running = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_running_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let schedstat_waiting = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_waiting_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let total_time = user_time + system_time + iowait_time + irq_time + nice_time + softirq_time + steal_time + idle_time + guest_user + guest_nice;
            let time = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            match mode
            {
                "normal" => {
                    println!("{:30} {:8} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             "all",
                             user_time / total_time * 100.,
                             nice_time / total_time * 100.,
                             system_time / total_time * 100.,
                             iowait_time / total_time * 100.,
                             steal_time / total_time * 100.,
                             idle_time / total_time * 100.
                    );
                },
                "all" => {
                    println!("{:30} {:8} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             "all",
                             user_time / total_time * 100.,
                             nice_time / total_time * 100.,
                             system_time / total_time * 100.,
                             iowait_time / total_time * 100.,
                             steal_time / total_time * 100.,
                             irq_time / total_time * 100.,
                             softirq_time / total_time * 100.,
                             guest_user / total_time * 100.,
                             guest_nice / total_time * 100.,
                             idle_time / total_time * 100.
                    );
                },
                "extended" => {
                    println!("{:30} {:8} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                             hostname,
                             time.format("%H:%M:%S"),
                             user_time,
                             nice_time,
                             system_time,
                             iowait_time,
                             steal_time,
                             irq_time,
                             softirq_time,
                             guest_user,
                             guest_nice,
                             idle_time,
                             schedstat_running,
                             schedstat_waiting,
                    );
                },
                &_ => {},
            }
        }
    }
}
pub fn print_sar_u_header(
   mode: &str,
)
{
    match mode
    {
        "normal" => {
            println!("{:30} {:8} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "CPU",
                     "%usr",
                     "%nice",
                     "%sys",
                     "%iowait",
                     "%steal",
                     "%idle",
            );
        }
        "all" => {
            println!("{:30} {:8} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "CPU",
                     "%usr",
                     "%nice",
                     "%sys",
                     "%iowait",
                     "%steal",
                     "%irq",
                     "%soft",
                     "%guest",
                     "%gnice",
                     "%idle",
            );
        },
        "extended" => {
            println!("{:30} {:8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                     "hostname",
                     "time",
                     "usr",
                     "nice",
                     "sys",
                     "iowait",
                     "steal",
                     "irq",
                     "soft",
                     "guest",
                     "gnice",
                     "idle",
                     "sch_run",
                     "sch_wait",
            );
        },
        &_ => {},
    }
}
