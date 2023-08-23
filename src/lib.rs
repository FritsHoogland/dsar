use std::time;
use prometheus_parse::Scrape;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::channel;
use time::Duration;
use log::*;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::node_cpu::NodeCpuDetails;
use crate::node_disk::NodeDiskDetails;
use crate::node_network::NodeNetworkDetails;
use crate::node_memory::NodeMemoryDetails;
use crate::yb_memory::YbMemoryDetails;
use crate::yb_io::YbIoDetails;
use crate::node_misc::NodeMiscDetails;

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

static LABEL_AREA_SIZE_LEFT: i32 = 100;
static LABEL_AREA_SIZE_RIGHT: i32 = 100;
static LABEL_AREA_SIZE_BOTTOM: i32 = 50;
static CAPTION_STYLE_FONT: &str = "monospace";
static CAPTION_STYLE_FONT_SIZE: i32 = 30;
static MESH_STYLE_FONT: &str = "monospace";
static MESH_STYLE_FONT_SIZE: i32 = 17;
static LABELS_STYLE_FONT: &str = "monospace";
static LABELS_STYLE_FONT_SIZE: i32 = 15;

#[derive(Debug, Default)]
pub struct Statistic {
    pub last_value: f64,
    pub delta_value: f64,
    pub per_second_value: f64,
    pub last_timestamp: DateTime<Utc>,
    pub first_value: bool,
}

#[derive(Debug, Default)]
pub struct HistoricalData {
    pub cpu_details: BTreeMap<(String, DateTime<Utc>), NodeCpuDetails>,
    pub disk_details: BTreeMap<(String, DateTime<Utc>, String), NodeDiskDetails>,
    pub network_details: BTreeMap<(String, DateTime<Utc>, String), NodeNetworkDetails>,
    pub memory_details: BTreeMap<(String, DateTime<Utc>), NodeMemoryDetails>,
    pub yb_memory_details: BTreeMap<(String, DateTime<Utc>), YbMemoryDetails>,
    pub yb_io_details: BTreeMap<(String, DateTime<Utc>), YbIoDetails>,
    pub misc_details: BTreeMap<(String, DateTime<Utc>), NodeMiscDetails>,
}

impl HistoricalData {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn add(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        self.add_node_cpu_statistics(statistics);
        self.add_node_disk_statistics(statistics);
        self.add_node_network_statistics(statistics);
        self.add_node_memory_statistics(statistics);
        self.add_yb_memory_statistics(statistics);
        self.add_yb_io_statistics(statistics);
        self.add_node_misc_statistics(statistics);
    }
    pub fn add_node_cpu_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, _, _), row)| host == hostname && metric == "node_cpu_seconds_total" && !row.first_value )
            {
                let timestamp = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                let user = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                let nice = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                let system = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "system" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                // node_cpu_seconds iowait mode doesn't exist on Mac
                let iowait = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "iowait" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_cpu_seconds steal mode doesn't exist on Mac
                let steal = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "steal" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_cpu_seconds irq mode doesn't exist on Mac
                let irq = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "irq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_cpu_seconds softirq mode doesn't exist on Mac
                let softirq = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "softirq" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_cpu_guest doesn't exist on Mac
                let guest_user = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "user" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_cpu_guest doesn't exist on Mac
                let guest_nice = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_guest_seconds_total" && mode == "nice" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let idle = statistics.iter().find(|((host, metric, cpu, mode), _)| host == hostname && metric == "node_cpu_seconds_total" && mode == "idle" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                // node_schedstat_running doesn't exist on Mac
                let schedstat_runtime = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_running_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                // node_schedstat_waiting doesn't exist on Mac
                let schedstat_wait = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_schedstat_waiting_seconds_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                self.cpu_details.entry((hostname.to_string(), timestamp)).or_insert(
                    NodeCpuDetails {
                        user,
                        nice,
                        system,
                        iowait,
                        steal,
                        irq,
                        softirq,
                        guest_user,
                        guest_nice,
                        idle,
                        schedstat_runtime,
                        schedstat_wait,
                    }
                );
            }
        }
    }
    pub fn add_node_network_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, _, _), row)| host == hostname && metric == "node_network_receive_packets_total" && !row.first_value )
            {
                for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_network_receive_packets_total").map(|((_, _, device, _), _)| device)
                {
                    let timestamp = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();

                    let receive_packets = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_packets = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_packets_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_bytes = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_bytes = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_compressed = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_compressed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_compressed = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_compressed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_multicast = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_multicast_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_errs = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_errs_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_errs = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_errs_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_colls = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_colls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_drop = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_drop_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_drop = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_drop_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_carrier = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_carrier_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let receive_fifo = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_receive_fifo_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let transmit_fifo = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_network_transmit_fifo_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();

                    let sockstat_sockets = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_sockets_used" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_tcp_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_TCP_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_udp_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_UDP_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_raw_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_RAW_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_frag_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_FRAG_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_tcp_tw = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_TCP_tw" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_tcp6_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_TCP6_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_udp6_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_UDP6_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_raw6_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_RAW6_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                    let sockstat_frag6_inuse = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_FRAG6_inuse" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();

                    let softnet_dropped = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_softnet_dropped_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let softnet_processed = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_softnet_processed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let softnet_times_squeezed = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_sockstat_times_squeezed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();

                    self.network_details.entry((hostname.to_string(), timestamp, current_device.to_string())).or_insert(
                        NodeNetworkDetails {
                            receive_packets,
                            transmit_packets,
                            receive_bytes,
                            transmit_bytes,
                            receive_compressed,
                            transmit_compressed,
                            receive_multicast,
                            receive_errs,
                            transmit_errs,
                            transmit_colls,
                            receive_drop,
                            transmit_drop,
                            transmit_carrier,
                            receive_fifo,
                            transmit_fifo,
                            sockstat_sockets,
                            sockstat_tcp_inuse,
                            sockstat_udp_inuse,
                            sockstat_raw_inuse,
                            sockstat_frag_inuse,
                            sockstat_tcp_tw,
                            sockstat_tcp6_inuse,
                            sockstat_udp6_inuse,
                            sockstat_raw6_inuse,
                            sockstat_frag6_inuse,
                            softnet_dropped,
                            softnet_processed,
                            softnet_times_squeezed,
                        }
                    );
                }
            }
        }
    }
    pub fn add_node_disk_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, _, _), row)| host == hostname && metric == "node_disk_read_bytes_total" && !row.first_value )
            {
                for current_device in statistics.iter().filter(|((host, metric, _, _), _)| host == hostname && metric == "node_disk_read_bytes_total").map(|((_, _, device, _), _)| device)
                {
                    let reads_completed_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let reads_bytes_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    // node_disk_reads_merged doesn't exist on Mac
                    let reads_merged_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_reads_merged_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let reads_time_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let reads_avg_latency_s = if (reads_time_s / reads_completed_s).is_nan() { 0. } else { reads_time_s / reads_completed_s };

                    let writes_completed_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let writes_bytes_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_written_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    // node_disk_writes_merged doesn't exist on Mac
                    let writes_merged_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_writes_merged_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let writes_time_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_write_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                    let writes_avg_latency_s = if (writes_time_s / writes_completed_s).is_nan() { 0. } else { writes_time_s / writes_completed_s };

                    // discards are not available with either centos 7 or an earlier node_exporter version
                    let discards_completed_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_discards_completed_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let discards_sectors_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_discarded_sectors_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let discards_merged_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_discards_merged_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let discards_time_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_discard_time_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let discards_avg_latency = if (discards_time_s / discards_completed_s).is_nan() { 0. } else { discards_time_s / discards_completed_s };

                    // xfs is per partition, disks are per disk
                    // but both share 'total'!
                    let xfs_read_calls_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_xfs_read_calls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let xfs_write_calls_s = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_xfs_write_calls_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();

                    // node_disk_io_time_weighted doesn't exist on Mac
                    let queue_size = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_io_time_weighted_seconds_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                    let timestamp = statistics.iter().find(|((host, metric, device, _), _)| host == hostname && metric == "node_disk_read_bytes_total" && device == current_device).map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();

                    self.disk_details.entry((hostname.to_string(), timestamp, current_device.to_string())).or_insert(
                        NodeDiskDetails {
                            reads_completed_s,
                            reads_bytes_s,
                            reads_avg_latency_s,
                            reads_merged_s,
                            writes_completed_s,
                            writes_bytes_s,
                            writes_avg_latency_s,
                            writes_merged_s,
                            discards_completed_s,
                            discards_sectors_s,
                            discards_avg_latency,
                            discards_merged_s,
                            queue_size,
                            xfs_read_calls_s,
                            xfs_write_calls_s,
                        }
                    );
                }
            }
        }
    }
    pub fn add_node_memory_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes" )
            {
                let active_anon = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_anon_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let active = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let active_file = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Active_file_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let anonhugepages = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_AnonHugePages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let anonpages = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_AnonPages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let bounce = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Bounce_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let buffers = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Buffers_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let cached = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Cached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let commitlimit = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_CommitLimit_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let committed_as = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Committed_AS_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let directmap2m = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_DirectMap2M_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                let directmap4k = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_DirectMap4k_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                let dirty = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Dirty_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let filehugepages = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_FileHugePages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let filepmdmapped = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_FilePmdMapped_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let hardwarecorrupted = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HardwareCorrupted_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default();
                let hugepages_free = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HugePages_Free").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let hugepages_rsvd = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HugePages_Rsvd").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let hugepages_surp = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HugePages_Surp").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let hugepages_total = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_HugePages_Total").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let hugepagesize = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Hugepagesize_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let hugetlb = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Hugetlb_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let inactive_anon = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_anon_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let inactive = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let inactive_file = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Inactive_file_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let kreclaimable = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_KReclaimable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let kernelstack = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_KernelStack_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let mapped = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Mapped_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let memavailable = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemAvailable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let memfree = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let memtotal = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let mlocked = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Mlocked_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let nfs_unstable = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_NFS_Unstable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let pagetables = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_PageTables_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let percpu = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Percpu_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let sreclaimable = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SReclaimable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let sunreclaim = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SUnreclaim_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let shmemhugepages = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_ShmemHugePages_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let shmempmdmapped = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_ShmemPmdMapped_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap_or_default(); // centos 7 / old node exporter
                let shmem = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Shmem_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let slab = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Slab_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let swapcached = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapCached_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let swapfree = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let swaptotal = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_SwapTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let unevictable = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Unevictable_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let vmallocchunk = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocChunk_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let vmalloctotal = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocTotal_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let vmallocused = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_VmallocUsed_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let writebacktmp = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_WritebackTmp_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let writeback = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_Writeback_bytes").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let timestamp = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_memory_MemFree_bytes").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                self.memory_details.entry((hostname.to_string(), timestamp)).or_insert(
                    NodeMemoryDetails{
                        active_anon,
                        active,
                        active_file,
                        anonhugepages,
                        anonpages,
                        bounce,
                        buffers,
                        cached,
                        commitlimit,
                        committed_as,
                        directmap2m,
                        directmap4k,
                        dirty,
                        filehugepages,
                        filepmdmapped,
                        hardwarecorrupted,
                        hugepages_free,
                        hugepages_rsvd,
                        hugepages_surp,
                        hugepages_total,
                        hugepagesize,
                        hugetlb,
                        inactive_anon,
                        inactive,
                        inactive_file,
                        kreclaimable,
                        kernelstack,
                        mapped,
                        memavailable,
                        memfree,
                        memtotal,
                        mlocked,
                        nfs_unstable,
                        pagetables,
                        percpu,
                        sreclaimable,
                        sunreclaim,
                        shmemhugepages,
                        shmempmdmapped,
                        shmem,
                        slab,
                        swapcached,
                        swapfree,
                        swaptotal,
                        unevictable,
                        vmallocchunk,
                        vmalloctotal,
                        vmallocused,
                        writebacktmp,
                        writeback,
                    }
                );
            }
        }
    }
    pub fn add_yb_memory_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server" )
            {
                let generic_heap = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let generic_allocated = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_current_allocated_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let tcmalloc_pageheap_free = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_free_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let tcmalloc_max_total_thread_cache = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_max_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let tcmalloc_current_total_thread_cache = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_current_total_thread_cache_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let tcmalloc_pageheap_unmapped = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "tcmalloc_pageheap_unmapped_bytes" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
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
                let mem_tracker_independent_allocs = mem_tracker_compressed_read_buffer_receive + mem_tracker_read_buffer_inbound_rpc_sending + mem_tracker_read_buffer_inbound_rpc_receive + mem_tracker_read_buffer_inbound_rpc_reading + mem_tracker_read_buffer_outbound_rpc_queueing + mem_tracker_read_buffer_outbound_rpc_receive + mem_tracker_read_buffer_outbound_rpc_sending + mem_tracker_read_buffer_outbound_rpc_reading;
                let timestamp = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "generic_heap_size" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                self.yb_memory_details.entry((hostname.to_string(), timestamp)).or_insert(
                    YbMemoryDetails {
                        generic_heap,
                        generic_allocated,
                        tcmalloc_pageheap_free,
                        tcmalloc_max_total_thread_cache,
                        tcmalloc_current_total_thread_cache,
                        tcmalloc_pageheap_unmapped,
                        mem_tracker,
                        mem_tracker_call,
                        mem_tracker_read_buffer,
                        mem_tracker_compressed_read_buffer,
                        mem_tracker_tablets,
                        mem_tracker_log_cache,
                        mem_tracker_blockbasedtable,
                        mem_tracker_independent_allocs,
                    }
                );
            }
        }
    }
    pub fn add_yb_io_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, metric_type, _), row)| host == hostname && metric == "glog_info_messages" && metric_type == "server" && !row.first_value )
            {
                let glog_info_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
                let glog_warning_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_warning_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default(); // statistic does not exist if no warnings have been generated
                let glog_error_messages = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_error_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default(); // probably the same for error messages
                let guaranteed_last_timestamp = statistics.iter().find(|((host, metric, metric_type, _), _)| host == hostname && metric == "glog_info_messages" && metric_type == "server").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();

                let log_bytes_logged = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_bytes_logged" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_reader_bytes_read = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_reader_bytes_read" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_sync_latency_count = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_sync_latency_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_sync_latency_sum = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_sync_latency_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_append_latency_count = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_append_latency_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_append_latency_sum = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_append_latency_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let log_cache_disk_reads = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "log_cache_disk_reads" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_flush_write_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_flush_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let intentsdb_rocksdb_flush_write_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_flush_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_compact_read_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_compact_read_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let intentsdb_rocksdb_compact_read_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_compact_read_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_compact_write_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_compact_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let intentsdb_rocksdb_compact_write_bytes = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_compact_write_bytes" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_write_raw_block_micros_count = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_write_raw_block_micros_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_write_raw_block_micros_sum = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_write_raw_block_micros_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_sst_read_micros_count = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_sst_read_micros_count" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_sst_read_micros_sum = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_sst_read_micros_sum" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let intentsdb_rocksdb_block_cache_hit = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_block_cache_hit" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let intentsdb_rocksdb_block_cache_miss = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "intentsdb_rocksdb_block_cache_miss" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_block_cache_hit = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_block_cache_hit" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                let rocksdb_block_cache_miss = statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && metric == "rocksdb_block_cache_miss" && statistic.last_timestamp == guaranteed_last_timestamp).map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
                self.yb_io_details.entry((hostname.to_string(), guaranteed_last_timestamp)).or_insert(
                    YbIoDetails {
                        glog_info_messages,
                        glog_warning_messages,
                        glog_error_messages,
                        log_bytes_logged,
                        log_reader_bytes_read,
                        log_sync_latency_count,
                        log_sync_latency_sum,
                        log_append_latency_count,
                        log_append_latency_sum,
                        log_cache_disk_reads,
                        rocksdb_flush_write_bytes,
                        intentsdb_rocksdb_flush_write_bytes,
                        rocksdb_compact_read_bytes,
                        intentsdb_rocksdb_compact_read_bytes,
                        rocksdb_compact_write_bytes,
                        intentsdb_rocksdb_compact_write_bytes,
                        rocksdb_write_raw_block_micros_count,
                        rocksdb_write_raw_block_micros_sum,
                        rocksdb_sst_read_micros_count,
                        rocksdb_sst_read_micros_sum,
                        intentsdb_rocksdb_block_cache_hit,
                        intentsdb_rocksdb_block_cache_miss,
                        rocksdb_block_cache_hit,
                        rocksdb_block_cache_miss,
                    }
                );
            }
        }
    }
    pub fn add_node_misc_statistics(
        &mut self,
        statistics: &BTreeMap<(String, String, String, String), Statistic>,
    )
    {
        for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
        {
            if statistics.iter().any(|((host, metric, _, _), row)| host == hostname && metric == "node_load1" && !row.first_value )
            {
                let timestamp = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
                let some_cpu = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_cpu_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let some_io = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_io_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let full_io = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_io_stalled_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let some_mem = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_memory_waiting_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let full_mem = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_pressure_memory_stalled_seconds_total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
                let load_1 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load1").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let load_5 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load5").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                let load_15 = statistics.iter().find(|((host, metric, _, _), _)| host == hostname && metric == "node_load15").map(|((_, _, _, _), statistic)| statistic.last_value).unwrap();
                self.misc_details.entry((hostname.to_string(), timestamp)).or_insert(
                    NodeMiscDetails {
                        some_cpu,
                        some_io,
                        full_io,
                        some_mem,
                        full_mem,
                        load_1,
                        load_5,
                        load_15,
                    }
                );
            }
        }
    }
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
                "node_pressure_cpu_waiting_seconds_total" |
                "node_pressure_io_stalled_seconds_total" |
                "node_pressure_io_waiting_seconds_total" |
                "node_pressure_memory_stalled_seconds_total" |
                "node_pressure_memory_waiting_seconds_total" |
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

                "intentsdb_rocksdb_block_cache_hit" |
                "intentsdb_rocksdb_block_cache_miss" |
                "rocksdb_block_cache_hit" |
                "rocksdb_block_cache_miss" |
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
        // this is highly inefficient currently, because it will visit every single statistic.
        // So statistics like single vcpu statistics, per disk statistics and per NIC statistics will all likely be re-revisited.
        for sample in &scrape.samples
        {
            node_cpu::create_total(sample, hostname, statistics);
            node_disk::create_total(sample, hostname, statistics);
            node_network::create_total(sample, hostname, statistics);
        };
    }
}

















