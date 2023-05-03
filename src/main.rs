use clap::{Parser, ValueEnum};
use tokio::time;
use time::Duration;
use anyhow::Result;
//use log::*;
use std::collections::BTreeMap;

use dsar::{read_node_exporter_into_map, process_statistics, Statistic, print_sar_u, print_sar_u_header, print_sar_d, print_sar_d_header, print_sar_n_dev, print_sar_n_dev_header, print_sar_n_edev, print_sar_n_edev_header, print_sar_r, print_sar_r_header, print_iostat, print_iostat_header, print_iostat_x, print_iostat_x_header, print_sar_s, print_sar_s_header, print_sar_w, print_sar_w_header, print_sar_b, print_sar_b_header, print_yb_cpu, print_yb_cpu_header, print_yb_network, print_yb_network_header, print_yb_memory, print_yb_memory_header, print_sar_q, print_sar_q_header, print_sar_n_sock, print_sar_n_sock_header, print_sar_n_sock6, print_sar_n_sock6_header, print_sar_n_soft, print_sar_n_soft_header, print_yb_io, print_yb_io_header, print_xfs_iops, print_xfs_iops_header};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputOptions
{
    SarU,
    #[clap(name = "sar-u-ALL")]
    SarUAll,
    SarD,
    #[clap(name = "sar-S")]
    SarS,
    #[clap(name = "sar-W")]
    SarW,
    SarQ,
    #[clap(name = "sar-B")]
    SarB,
    #[clap(name = "sar-n-DEV")]
    SarNDev,
    #[clap(name = "sar-n-EDEV")]
    SarNEdev,
    #[clap(name = "sar-n-SOCK")]
    SarNSock,
    #[clap(name = "sar-n-SOCK6")]
    SarNSock6,
    #[clap(name = "sar-n-SOFT")]
    SarNSoft,
    SarR,
    #[clap(name = "sar-r-ALL")]
    SarRAll,
    Iostat,
    IostatX,
    YbCpu,
    YbNetwork,
    YbMemory,
    YbIo,
    CpuAll,
    XfsIops,
}

#[derive(Debug, Parser)]
#[clap(version, about, long_about = None)]
pub struct Opts
{
    /// Hostnames
    #[arg(short = 'H', long, value_name = "hostname,hostname")]
    hosts: String,
    /// Ports
    #[arg(short = 'P', long, value_name = "port,port", default_value = "9000,9300")]
    ports: String,
    /// Interval
    #[arg(short = 'i', long, value_name = "time (s)", default_value = "1")]
    interval: u64,
    /// Parallel
    #[arg(short = 'p', long, value_name = "parallel", default_value = "3")]
    parallel: usize,
    /// Print header
    #[arg(short = 'n', long, value_name = "nr", default_value = "5")]
    header_print: u64,
    /// Output
    #[arg(short = 'o', long, value_name = "option", value_enum, default_value_t = OutputOptions::SarU )]
    output: OutputOptions,
}

#[tokio::main]
async fn main() -> Result<()> 
{
    env_logger::init();
    let args = Opts::parse();

    let mut interval = time::interval(Duration::from_secs(args.interval));
    let mut statistics: BTreeMap<(String, String, String, String), Statistic> = Default::default();


    let mut print_counter: u64 = 0;
    loop
    {
        interval.tick().await;
        let node_exporter_values = read_node_exporter_into_map(&args.hosts.split(',').collect(), &args.ports.split(',').collect(), args.parallel).await;
        process_statistics(&node_exporter_values, &mut statistics).await;
        if print_counter == 0 || print_counter % args.header_print == 0
        {
            match args.output {
                OutputOptions::SarU => print_sar_u_header("normal"),
                OutputOptions::SarUAll => print_sar_u_header("all"),
                OutputOptions::SarD => print_sar_d_header(),
                OutputOptions::SarS => print_sar_s_header(),
                OutputOptions::SarW => print_sar_w_header(),
                OutputOptions::SarB => print_sar_b_header(),
                OutputOptions::SarQ => print_sar_q_header(),
                OutputOptions::SarNDev => print_sar_n_dev_header(),
                OutputOptions::SarNEdev => print_sar_n_edev_header(),
                OutputOptions::SarNSock => print_sar_n_sock_header(),
                OutputOptions::SarNSock6 => print_sar_n_sock6_header(),
                OutputOptions::SarNSoft => print_sar_n_soft_header(),
                OutputOptions::SarR => print_sar_r_header("normal"),
                OutputOptions::SarRAll => print_sar_r_header("all"),
                OutputOptions::Iostat => print_iostat_header(),
                OutputOptions::IostatX => print_iostat_x_header(),
                OutputOptions::YbCpu => print_yb_cpu_header(),
                OutputOptions::YbNetwork => print_yb_network_header(),
                OutputOptions::YbMemory => print_yb_memory_header(),
                OutputOptions::YbIo => print_yb_io_header(),
                OutputOptions::CpuAll => print_sar_u_header("extended"),
                OutputOptions::XfsIops => print_xfs_iops_header(),
            }
        };
        match args.output {
            OutputOptions::SarU => print_sar_u("normal", &statistics),
            OutputOptions::SarUAll => print_sar_u("all", &statistics),
            OutputOptions::SarD => print_sar_d(&statistics),
            OutputOptions::SarS => print_sar_s(&statistics),
            OutputOptions::SarW => print_sar_w(&statistics),
            OutputOptions::SarB => print_sar_b(&statistics),
            OutputOptions::SarQ => print_sar_q(&statistics),
            OutputOptions::SarNDev => print_sar_n_dev(&statistics),
            OutputOptions::SarNEdev => print_sar_n_edev(&statistics),
            OutputOptions::SarNSock => print_sar_n_sock(&statistics),
            OutputOptions::SarNSock6 => print_sar_n_sock6(&statistics),
            OutputOptions::SarNSoft => print_sar_n_soft(&statistics),
            OutputOptions::SarR => print_sar_r("normal", &statistics),
            OutputOptions::SarRAll => print_sar_r("all", &statistics),
            OutputOptions::Iostat => print_iostat(&statistics),
            OutputOptions::IostatX => print_iostat_x(&statistics),
            OutputOptions::YbCpu => print_yb_cpu(&statistics),
            OutputOptions::YbNetwork => print_yb_network(&statistics),
            OutputOptions::YbMemory => print_yb_memory(&statistics),
            OutputOptions::YbIo => print_yb_io(&statistics),
            OutputOptions::CpuAll => print_sar_u("extended", &statistics),
            OutputOptions::XfsIops => print_xfs_iops(&statistics),
        }
        print_counter += 1;
    }
}
