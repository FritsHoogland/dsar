use std::{collections, sync, time};
use prometheus_parse::Scrape;
use collections::HashMap;
use std::collections::BTreeMap;
use sync::mpsc::channel;
use time::Duration;
use log::*;
use chrono::{DateTime, Utc};

pub mod node_cpu;
pub mod node_disk;
pub mod node_network;
pub mod node_memory;
pub mod node_vmstat;
pub mod node_misc;
pub mod yb_cpu;
pub mod yb_network;
pub mod yb_memory;
pub mod yb_io;

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
                        let node_exporter_values = read_node_exporter(host, port, endpoint);
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
                "node_schedstat_running_seconds_total" |
                "node_schedstat_waiting_seconds_total" |
                "node_cpu_seconds_total" |
                "node_cpu_guest_seconds_total" => node_cpu::process_statistic(sample, hostname, statistics),

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
                "node_xfs_write_calls_total" => node_disk::process_statistic(sample, hostname, statistics),

                "node_network_receive_packets_total" |
                "node_network_transmit_packets_total" |
                "node_network_receive_bytes_total" |
                "node_network_transmit_bytes_total" |
                "node_network_receive_compressed_total" |
                "node_network_transmit_compressed_total" |
                "node_network_receive_multicast_total" |
                "node_network_receive_errs_total" |
                "node_network_transmit_errs_total" |
                "node_network_transmit_colls_total" |
                "node_network_receive_drop_total" |
                "node_network_transmit_drop_total" |
                "node_network_transmit_carrier_total" |
                "node_network_receive_fifo_total" |
                "node_network_transmit_fifo_total" |
                "node_sockstat_sockets_used" |
                "node_sockstat_TCP_inuse" |
                "node_sockstat_UDP_inuse" |
                "node_sockstat_RAW_inuse" |
                "node_sockstat_FRAG_inuse" |
                "node_sockstat_TCP_tw" |
                "node_sockstat_TCP6_inuse" |
                "node_sockstat_UDP6_inuse" |
                "node_sockstat_RAW6_inuse" |
                "node_sockstat_FRAG6_inuse" |
                "node_softnet_dropped_total" |
                "node_softnet_processed_total" |
                "node_softnet_times_squeezed_total" => node_network::process_statistic(sample, hostname, statistics),

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
                "node_memory_Writeback_bytes" => node_memory::process_statistic(sample, hostname, statistics),

                "node_vmstat_oom_kill" |
                "node_vmstat_pgfault" |
                "node_vmstat_pgmajfault" |
                "node_vmstat_pgpgin" |
                "node_vmstat_pgpgout" |
                "node_vmstat_pswpin" |
                "node_vmstat_pswpout" => node_vmstat::process_statistic(sample, hostname, statistics),

                "node_procs_running" |
                "node_procs_blocked" |
                "node_load1" |
                "node_load5" |
                "node_load15" |
                "node_intr_total" |
                "node_context_switches_total" => node_misc::process_statistic(sample, hostname, statistics),

                "cpu_stime" |
                "cpu_utime" |
                "voluntary_context_switches" |
                "involuntary_context_switches" => yb_cpu::process_statistic(sample, hostname, statistics),

                "tcp_bytes_received" |
                "tcp_bytes_sent" => yb_network::process_statistic(sample, hostname, statistics),

                "generic_heap_size" |
                "generic_current_allocated_bytes" |
                "tcmalloc_pageheap_free_bytes" |
                "tcmalloc_max_total_thread_cache_bytes" |
                "tcmalloc_current_total_thread_cache_bytes" |
                "tcmalloc_pageheap_unmapped_bytes" |
                "mem_tracker" |
                "mem_tracker_Call" |
                "mem_tracker_Call_Outbound_RPC" |
                "mem_tracker_Call_Inbound_RPC" |
                "mem_tracker_Call_Redis" |
                "mem_tracker_Call_CQL" |
                "mem_tracker_Read_Buffer" |
                "mem_tracker_Read_Buffer_Inbound_RPC" |
                "mem_tracker_Read_Buffer_Inbound_RPC_Sending" |
                "mem_tracker_Read_Buffer_Inbound_RPC_Receive" |
                "mem_tracker_Read_Buffer_Inbound_RPC_Reading" |
                "mem_tracker_Read_Buffer_Outbound_RPC" |
                "mem_tracker_Read_Buffer_Outbound_RPC_Queueing" |
                "mem_tracker_Read_Buffer_Outbound_RPC_Receive" |
                "mem_tracker_Read_Buffer_Outbound_RPC_Sending" |
                "mem_tracker_Read_Buffer_Outbound_RPC_Reading" |
                "mem_tracker_Read_Buffer_Redis" |
                "mem_tracker_Read_Buffer_Redis_Allocated" |
                "mem_tracker_Read_Buffer_Redis_Used" |
                "mem_tracker_Read_Buffer_Redis_Mandatory" |
                "mem_tracker_Read_Buffer_CQL" |
                "mem_tracker_Compressed_Read_Buffer" |
                "mem_tracker_Compressed_Read_Buffer_Receive" |
                "mem_tracker_BlockBasedTable" |
                "mem_tracker_BlockBasedTable_IntentsDB" |
                "mem_tracker_BlockBasedTable_RegularDB" |
                "mem_tracker_log_cache" |
                "mem_tracker_Tablets" |
                "mem_tracker_Tablets_transactions" => yb_memory::process_statistic(sample, hostname, statistics),

                "glog_info_messages" |
                "glog_warning_message" |
                "glog_error_messages" |
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
                "rocksdb_sst_read_micros_sum" => yb_io::process_statistic(sample, hostname, statistics),

                &_ => {},
            }
        }
        for sample in &scrape.samples
        {
            node_cpu::create_total(sample, hostname, statistics);
            node_disk::create_total(sample, hostname, statistics);
            node_network::create_total(sample, hostname, statistics);
        };
    }
}

















