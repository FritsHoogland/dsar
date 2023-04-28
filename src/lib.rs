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

pub async fn process_cpu_statistics(
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
                "node_procs_running" | "node_procs_blocked" | "node_load_1" | "node_load_5" | "node_load_15" |
                "node_memory_MemFree_bytes" | "node_memory_MemAvailable_bytes" | "node_memory_MemTotal_bytes" |
                "node_memory_Buffers_bytes" | "node_memory_Cached_bytes" | "node_memory_Committed_AS_bytes" | "node_memory_SwapTotal_bytes" |
                "node_memory_SwapCached_bytes" | "node_memory_SwapFree_bytes" |
                "node_memory_Active_bytes" | "node_memory_Inactive_bytes" | "node_memory_Dirty_bytes" | "node_memory_AnonPages_bytes" |
                "node_memory_Slab_bytes" | "node_memory_KernelStack_bytes" | "node_memory_PageTables_bytes" | "node_memory_VmallocTotal_bytes" => {
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
                "node_intr_total" | "node_context_switches_total" => {
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
                            row.per_second_value = row.delta_value / sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.;
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
                "node_vmstat_pgfault" | "node_vmstat_pgmajfault" | "node_vmstat_pgpgin" | "node_vmstat_pgpgout" | "node_vmstat_pswpin" | "node_vmstat_pswpout" => {
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
                            row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.);
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
                // YB statistics. These are untyped, but are actually counters.
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
                "generic_heap_size" |
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
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value } )
                        .or_insert( Statistic { per_second_value:  per_second_value, ..Default::default() });
                }
            };
            // node_cpu_seconds_total
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_seconds_total").count() > 0
            {
                for mode in vec!["idle", "iowait", "irq", "nice", "softirq", "steal", "system", "user"]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    statistics.entry( (hostname.to_string(), "node_cpu_seconds_total".to_string(), "total".to_string(), mode.to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value } )
                        .or_insert( Statistic { per_second_value:  per_second_value, ..Default::default() });
                }
            };
            // node_cpu_guest_seconds_total
            if statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_cpu_guest_seconds_total").count() > 0
            {
                for mode in vec!["nice", "user" ]
                {
                    let per_second_value = statistics.iter().filter(|((host, metric, cpu, run_mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && cpu != "total" && run_mode == mode).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                    statistics.entry( (hostname.to_string(), "node_cpu_guest_seconds_total".to_string(), "total".to_string(), mode.to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value } )
                        .or_insert( Statistic { per_second_value:  per_second_value, ..Default::default() });
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
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value } )
                        .or_insert( Statistic { per_second_value:  per_second_value, ..Default::default() });
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
                    statistics.entry( (hostname.to_string(), metric_name.to_string(), "total".to_string(), "".to_string()))
                        .and_modify(|row| { row.per_second_value = per_second_value } )
                        .or_insert( Statistic { per_second_value:  per_second_value, ..Default::default() });
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
            println!("{:30} {:10.0} {:10.0} {:10.2} {:10.0} {:10.2}",
                     hostname,
                     swap_free / (1024. * 1024.),
                     swap_used / (1024. * 1024.),
                     swap_used_percent,
                     swap_cached / (1024. * 1024.),
                     swap_cached_percent,
            );
        }
    }
}

pub fn print_sar_s_header()
{
    println!("{:30} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
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
            println!("{:30} {:10.0} {:10.0}",
                     hostname,
                     pages_swap_in,
                     pages_swap_out,
            );
        }
    }
}

pub fn print_sar_w_header()
{
    println!("{:30} {:>10} {:>10}",
             "hostname",
             "pswpin/s",
             "pswpout/s",
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
            println!("{:50} {:10.3} {:10.3} {:10.3}",
                     hostname,
                     cpu_user / 1000.,
                     cpu_system / 1000.,
                     cpu_total / 1000.,
            );
        }
    }
}

pub fn print_yb_cpu_header()
{
    println!("{:50} {:>10} {:>10} {:>10}",
             "hostname",
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
            println!("{:50} {:10.2} {:10.2} {:10.2}",
                     hostname,
                     network_bytes_received / (1024.*1024.),
                     network_bytes_sent / (1024.*1024.),
                     network_bytes_total / (1024.*1024.),
            );
        }
    }
}

pub fn print_yb_network_header()
{
    println!("{:50} {:>10} {:>10} {:>10}",
             "hostname",
             "rxMB/s",
             "txMB/s",
             "totMB/s",
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
            println!("{:30} {:10.2} {:10.2} {:10.2} {:10.2}",
                     hostname,
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
    println!("{:30} {:>10} {:>10} {:>10} {:>10}",
             "hostname",
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
            if mode == "normal"
            {
                println!("{:30} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0}",
                         hostname,
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
                println!("{:30} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.2} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0} {:9.0}",
                         hostname,
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
        println!("{:30} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
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
        println!("{:30} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
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
                 "mbmbslab",
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
                println!("{:30} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}", hostname, current_device, receive_errors, transmit_errors, transmit_collisions, receive_drop, transmit_drop, transmit_carrier, receive_fifo, transmit_fifo);
            }
        }
    }
}

pub fn print_sar_n_edev_header()
{
    println!("{:30} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
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
                println!("{:30} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}", hostname, current_device, receive_packets, transmit_packets, receive_bytes / (1024. * 1024.), transmit_bytes / (1024. * 1024.), compressed_packets_received, compressed_packets_transmit, multicast_packets_received);
            }
        }
    }
}

pub fn print_sar_n_dev_header()
{
    println!("{:30} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
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
                println!("{:30} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}", hostname, current_device, tps, read_bytes / (1024. * 1024.), write_bytes / (1024. * 1024.), (average_read_request_size + average_write_request_size) / (1024. * 1024.), queue_size, (average_read_request_time_ms + average_write_request_time_ms) / 2.);
            }
        }
    }
}

pub fn print_sar_d_header()
{
    println!("{:30} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
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
                println!("{:30} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}", hostname, current_device, tps, read_bytes / (1024. * 1024.), write_bytes / (1024. * 1024.), read_total / (1024. * 1024.), write_total / (1024. * 1024.));
            }
        }
    }
}

pub fn print_iostat_header()
{
    println!("{:30} {:10} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
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


                println!("{:30} {:10} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
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
    println!("{:30} {:10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
             "hostname",
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
            let user_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let system_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "system" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let iowait_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "iowait" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let irq_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "irq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let nice_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let softirq_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "softirq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let steal_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "steal" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let idle_time = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "idle" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let guest_user = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let guest_nice = statistics.iter().filter(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).next().unwrap();
            let total_time = user_time + system_time + iowait_time + irq_time + nice_time + softirq_time + steal_time + idle_time + guest_user + guest_nice;
            if mode == "normal"
            {
                println!("{:30} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
                         "all",
                         user_time / total_time * 100.,
                         nice_time / total_time * 100.,
                         system_time / total_time * 100.,
                         iowait_time / total_time * 100.,
                         steal_time / total_time * 100.,
                         idle_time / total_time * 100.
                );
            } else {
                println!("{:30} {:3} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2} {:9.2}",
                         hostname,
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
            }
        }
    }
}
pub fn print_sar_u_header(
   mode: &str,
)
{
    if mode == "normal"
    {
        println!("{:30} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
                 "CPU",
                 "%usr",
                 "%nice",
                 "%sys",
                 "%iowait",
                 "%steal",
                 "%idle"
        );
    }
    else
    {
        println!("{:30} {:3} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                 "hostname",
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
                 "%idle"
        );
    }
}
