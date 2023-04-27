use clap::Parser;
use tokio::time;
use time::Duration;
use anyhow::Result;
//use log::*;
use std::collections::BTreeMap;

use dsar::{read_node_exporter_into_map, process_cpu_statistics, Statistic, print_sar_u, print_sar_u_header, print_sar_d, print_sar_d_header, print_sar_n_dev, print_sar_n_dev_header, print_sar_n_edev, print_sar_n_edev_header};

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
    #[arg(short = 'n', long, value_name = "nr", default_value = "10")]
    header_print: u64,
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
        let node_exporter_values = read_node_exporter_into_map(&args.hosts.split(",").collect(), &args.ports.split(",").collect(), args.parallel).await;
        process_cpu_statistics(&node_exporter_values, &mut statistics).await;
        if print_counter == 0 || print_counter % args.header_print == 0
        {
            //print_sar_u_header("all");
            //print_sar_d_header();
            //print_sar_n_dev_header();
            print_sar_n_edev_header();
        };
        //print_sar_u("all", &statistics);
        //print_sar_d(&statistics);
        //print_sar_n_dev(&statistics);
        print_sar_n_edev(&statistics);
        print_counter += 1;
    }
}
