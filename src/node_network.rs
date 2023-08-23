use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use prometheus_parse::{Value, Sample};
use itertools::Itertools;
use log::*;
use plotters::prelude::*;
use plotters::chart::SeriesLabelPosition::UpperLeft;

use crate::{Statistic, HistoricalData, LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE, LABEL_AREA_SIZE_LEFT, LABEL_AREA_SIZE_BOTTOM, LABEL_AREA_SIZE_RIGHT, CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE, MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE};

#[derive(Debug)]
pub struct NodeNetworkDetails {
    pub receive_packets: f64,
    pub transmit_packets: f64,
    pub receive_bytes: f64,
    pub transmit_bytes: f64,
    pub receive_compressed: f64,
    pub transmit_compressed: f64,
    pub receive_multicast: f64,
    pub receive_errs: f64,
    pub transmit_errs: f64,
    pub transmit_colls: f64,
    pub receive_drop: f64,
    pub transmit_drop: f64,
    pub transmit_carrier: f64,
    pub receive_fifo: f64,
    pub transmit_fifo: f64,
    pub sockstat_sockets: f64,
    pub sockstat_tcp_inuse: f64,
    pub sockstat_udp_inuse: f64,
    pub sockstat_raw_inuse: f64,
    pub sockstat_frag_inuse: f64,
    pub sockstat_tcp_tw: f64,
    pub sockstat_tcp6_inuse: f64,
    pub sockstat_udp6_inuse: f64,
    pub sockstat_raw6_inuse: f64,
    pub sockstat_frag6_inuse: f64,
    pub softnet_dropped: f64,
    pub softnet_processed: f64,
    pub softnet_times_squeezed: f64,
}
pub fn process_statistic(
    sample: &Sample,
    hostname: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
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
        "node_network_transmit_fifo_total" => {
            let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
            let device = sample.labels.iter().find(|(label, _)| *label == "device").map(|(_, value)| value).unwrap();
            if device.eq("lo") { return };
            statistics
                .entry(( hostname.to_string(), sample.metric.clone(), device.to_string(), "".to_string() ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    row.first_value = false;
                    debug!("{} device: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", sample.metric, device, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
        "node_sockstat_sockets_used" |
        "node_sockstat_TCP_inuse" |
        "node_sockstat_UDP_inuse" |
        "node_sockstat_RAW_inuse" |
        "node_sockstat_FRAG_inuse" |
        "node_sockstat_TCP_tw" |
        "node_sockstat_TCP6_inuse" |
        "node_sockstat_UDP6_inuse" |
        "node_sockstat_RAW6_inuse" |
        "node_sockstat_FRAG6_inuse" => {
            let Value::Gauge(value) = sample.value else { panic!("{} value enum type should be Gauge!", sample.metric)};
            statistics
                .entry((
                    hostname.to_string(),
                    sample.metric.clone(),
                    "".to_string(),
                    "".to_string(),
                ))
                .and_modify( |row| {
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    row.first_value = false;
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
        "node_softnet_dropped_total" |
        "node_softnet_processed_total" |
        "node_softnet_times_squeezed_total" => {
            let Value::Counter(value) = sample.value else { panic!("{} value enum type should be Counter!", sample.metric)};
            let cpu = sample.labels.iter().find(|(label, _)| *label == "cpu").map(|(_, value)| value).unwrap();
            statistics
                .entry(( hostname.to_string(), sample.metric.clone(), cpu.to_string(), "".to_string() ))
                .and_modify( |row| {
                    row.delta_value = value - row.last_value;
                    row.per_second_value = row.delta_value / (sample.timestamp.signed_duration_since(row.last_timestamp).num_milliseconds() as f64 / 1000.0);
                    row.last_value = value;
                    row.last_timestamp = sample.timestamp;
                    row.first_value = false;
                    debug!("{}: metric: {} cpu: {}, last_value: {}, last_timestamp: {}, delta_value: {}, per_second_value: {}", hostname, sample.metric, cpu, row.last_value, row.last_timestamp, row.delta_value, row.per_second_value);
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
        &_ => {},
    }
}

pub fn create_total(
    sample: &Sample,
    host: &str,
    statistics: &mut BTreeMap<(String, String, String, String), Statistic>,
)
{
    match sample.metric.as_str()
    {
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
        "node_network_transmit_fifo_total" => {
            let last_timestamp = statistics.iter().find(|((hostname, metric, device, _), _)| hostname == host && metric == &sample.metric && device != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            let per_second_value = statistics.iter().filter(|((hostname, metric, device, _), _)| hostname == host && metric == &sample.metric && device != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let first_val = statistics.iter().find(|((hostname, metric, device, _), _)| hostname == host && metric == &sample.metric && device != "total").map(|((_, _, _, _), statistic)| statistic.first_value).unwrap();
            statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), "".to_string()))
                .and_modify(|row| {
                    row.per_second_value = per_second_value;
                    row.last_timestamp = last_timestamp;
                    row.first_value = first_val;
                })
                .or_insert(Statistic {
                    per_second_value,
                    last_timestamp,
                    first_value: first_val,
                    ..Default::default()
                });
        },
        "node_softnet_dropped_total" |
        "node_softnet_processed_total" |
        "node_softnet_times_squeezed_total" => {
            let last_timestamp = statistics.iter().find(|((hostname, metric, cpu, _), _)| hostname == host && metric == &sample.metric && cpu != "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
            let per_second_value = statistics.iter().filter(|((hostname, metric, cpu, _), _)| hostname == host && metric == &sample.metric && cpu != "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).sum();
            let first_val = statistics.iter().find(|((hostname, metric, cpu, _), _)| hostname == host && metric == &sample.metric && cpu != "total").map(|((_, _, _, _), statistic)| statistic.first_value).unwrap();
            statistics.entry((host.to_string(), sample.metric.to_string(), "total".to_string(), "".to_string()))
                .and_modify(|row| {
                    row.per_second_value = per_second_value;
                    row.last_timestamp = last_timestamp;
                    row.first_value = first_val;
                })
                .or_insert(Statistic {
                    per_second_value,
                    last_timestamp,
                    first_value: first_val,
                    ..Default::default()
                });
        }
        &_ => {},
    }
}

pub fn print_sar_n_soft(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && !statistic.first_value && metric == "node_softnet_processed_total").count() > 0
        {
            let soft_total = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_processed_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let soft_dropped = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_dropped_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let soft_squeezed = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_times_squeezed_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap();
            let soft_interproc_intr= statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_interpoc_intr_doesnotexist" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let soft_flow_limit = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_flow_limit_doesnotexist" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.per_second_value).unwrap_or_default();
            let time = statistics.iter().find(|((host, metric, cpu, _), _)| host == hostname && metric == "node_softnet_processed_total" && cpu == "total").map(|((_, _, _, _), statistic)| statistic.last_timestamp).unwrap();
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
                         transmit_fifo,
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
             "txfifo/s",
    );
}

pub fn print_sar_n_dev(
    statistics: &BTreeMap<(String, String, String, String), Statistic>,
)
{
    for hostname in statistics.iter().map(|((hostname, _, _, _), _)| hostname).unique()
    {
        if statistics.iter().filter(|((host, metric, _, _), statistic)| host == hostname && !statistic.first_value && metric == "node_network_receive_packets_total").count() > 0
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
                         multicast_packets_received,
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
             "rxmcst/s",
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

pub fn create_network_plots(
    historical_data: &Arc<Mutex<HistoricalData>>,
)
{
    let unlocked_historical_data = historical_data.lock().unwrap();
    for filter_hostname in unlocked_historical_data.network_details.keys().map(|(hostname, _, _)| hostname).unique()
    {
        for current_device in unlocked_historical_data.network_details.iter().filter(|((hostname, _, _), _)| hostname == filter_hostname).map(|((_, _, device), _)| device).unique()
        {
            let number_of_areas = 2;
            let y_size_of_root = 1400;

            let filename = format!("{}_network_{}.png", filter_hostname, current_device);
            let root = BitMapBackend::new(&filename, (1280, y_size_of_root)).into_drawing_area();
            let multiroot = root.split_evenly((number_of_areas, 1));


            // packets plot
            // set the plot specifics
            let start_time = unlocked_historical_data.network_details
                .keys()
                .filter(|(hostname, _, device)| hostname == filter_hostname && device == current_device)
                .map(|(_, timestamp, _)| timestamp)
                .min()
                .unwrap();
            let end_time = unlocked_historical_data.network_details
                .keys()
                .filter(|(hostname, _, device)| hostname == filter_hostname && device == current_device)
                .map(|(_, timestamp, _)| timestamp)
                .max()
                .unwrap();

            // packets plot
            let low_value_packets: f64 = 0.0;
            let high_value_packets = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.receive_packets + row.transmit_packets)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            multiroot[0].fill(&WHITE).unwrap();
            let mut contextarea = ChartBuilder::on(&multiroot[0])
                .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
                .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
                .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
                .caption(format!("network packets per second: {} {}", filter_hostname, current_device), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
                .build_cartesian_2d(*start_time..*end_time, low_value_packets..high_value_packets)
                .unwrap();
            contextarea.configure_mesh()
                .x_labels(4)
                .x_label_formatter(&|x| x.to_rfc3339())
                .y_desc("packets per second")
                .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
                .draw()
                .unwrap();
            let min_receive_packets = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.receive_packets )
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_receive_packets = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.receive_packets )
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.network_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, row.receive_packets + row.transmit_packets)), 0.0, Palette99::pick(1))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Receive packets/s", min_receive_packets, max_receive_packets))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
            let min_transmit_packets = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.transmit_packets )
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_transmit_packets = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| row.transmit_packets )
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.network_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, row.transmit_packets)), 0.0, Palette99::pick(2))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Transmit packets/s", min_transmit_packets, max_transmit_packets))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
            contextarea.configure_series_labels()
                .border_style(BLACK)
                .background_style(WHITE.mix(0.7))
                .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
                .position(UpperLeft)
                .draw()
                .unwrap();

            // megabit plot
            let low_value_mbit_second: f64 = 0.0;
            let high_value_mbit_second = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| ((row.receive_bytes + row.transmit_bytes) / (1024.*1024.)) * 8. )
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            multiroot[1].fill(&WHITE).unwrap();
            let mut contextarea = ChartBuilder::on(&multiroot[1])
                .set_label_area_size(LabelAreaPosition::Left, LABEL_AREA_SIZE_LEFT)
                .set_label_area_size(LabelAreaPosition::Bottom, LABEL_AREA_SIZE_BOTTOM)
                .set_label_area_size(LabelAreaPosition::Right, LABEL_AREA_SIZE_RIGHT)
                .caption(format!("network megabit per second: {} {}", filter_hostname, current_device), (CAPTION_STYLE_FONT, CAPTION_STYLE_FONT_SIZE))
                .build_cartesian_2d(*start_time..*end_time, low_value_mbit_second..high_value_mbit_second)
                .unwrap();
            contextarea.configure_mesh()
                .x_labels(4)
                .x_label_formatter(&|x| x.to_rfc3339())
                .y_desc("megabit per second")
                .label_style((MESH_STYLE_FONT, MESH_STYLE_FONT_SIZE))
                .draw()
                .unwrap();
            let min_receive_mbit_s = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| (row.receive_bytes / (1024.*1024.)) * 8. )
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_receive_mbit_s = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| (row.receive_bytes / (1024.*1024.)) * 8. )
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.network_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, ((row.receive_bytes + row.transmit_bytes) / (1024.*1024.)) * 8. )), 0.0, Palette99::pick(1))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Receive mbit/s", min_receive_mbit_s, max_receive_mbit_s))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(1).filled()));
            let min_transmit_mbit_s = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| (row.transmit_bytes / (1024.*1024.)) * 8. )
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_transmit_mbit_s = unlocked_historical_data.network_details.iter()
                .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                .map(|((_, _, _), row)| (row.transmit_bytes / (1024.*1024.)) * 8. )
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            contextarea.draw_series(AreaSeries::new(unlocked_historical_data.network_details.iter()
                                                        .filter(|((hostname, _, device), _)| hostname == filter_hostname && device == current_device)
                                                        .map(|((_, timestamp, _), row)| (*timestamp, (row.transmit_packets / (1024.*1024.)) * 8. )), 0.0, Palette99::pick(2))
            )
                .unwrap()
                .label(format!("{:25} min: {:10.2}, max: {:10.2}", "Transmit mbit/s", min_transmit_mbit_s, max_transmit_mbit_s))
                .legend(move |(x, y)| Rectangle::new([(x - 3, y - 3), (x + 3, y + 3)], Palette99::pick(2).filled()));
            contextarea.configure_series_labels()
                .border_style(BLACK)
                .background_style(WHITE.mix(0.7))
                .label_font((LABELS_STYLE_FONT, LABELS_STYLE_FONT_SIZE))
                .position(UpperLeft)
                .draw()
                .unwrap();
        }
    }
}
