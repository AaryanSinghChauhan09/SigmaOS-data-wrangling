//! CLI for sigmaos-data-wrangling.

use clap::{Parser, Subcommand};
use serde_json::json;
use sigmaos_data_wrangling::{
    load_pipeline, load_records, run, save_pipeline, Pipeline, Transform,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sigmaos-data-wrangling", about = "SigmaOS ETL pipeline engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline JSON file.
    Run { file: PathBuf },
    /// Run ad-hoc transforms on a JSON file.
    Transform {
        input: PathBuf,
        output: PathBuf,
        /// e.g. --filter status=active --limit 10 --sort age
        #[arg(long)]
        filter: Vec<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        sort: Option<String>,
    },
    /// Show records from a JSON file.
    Show { file: PathBuf },
    /// Save a pipeline template.
    SaveTemplate { file: PathBuf },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    match Cli::parse().command {
        Commands::Run { file } => {
            let p = load_pipeline(&file)?;
            let records = run(&p)?;
            println!("pipeline '{}' produced {} records", p.name, records.len());
        }
        Commands::Transform {
            input,
            output,
            filter,
            limit,
            sort,
        } => {
            let mut steps = Vec::new();
            for f in filter {
                if let Some((k, v)) = f.split_once('=') {
                    steps.push(Transform::Filter {
                        field: k.to_string(),
                        equals: v.to_string(),
                    });
                }
            }
            if let Some(s) = sort {
                steps.push(Transform::Sort { field: s });
            }
            if let Some(n) = limit {
                steps.push(Transform::Limit { n });
            }
            let p = Pipeline {
                name: "adhoc".into(),
                input,
                output,
                steps,
            };
            let records = run(&p)?;
            println!("{} records", records.len());
        }
        Commands::Show { file } => {
            let records = load_records(&file)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({"count": records.len(), "records": records}))?
            );
        }
        Commands::SaveTemplate { file } => {
            let p = Pipeline {
                name: "example".into(),
                input: PathBuf::from("input.json"),
                output: PathBuf::from("output.json"),
                steps: vec![
                    Transform::Filter {
                        field: "status".into(),
                        equals: "active".into(),
                    },
                    Transform::Sort {
                        field: "created".into(),
                    },
                    Transform::Limit { n: 100 },
                ],
            };
            save_pipeline(&p, &file)?;
            println!("saved template: {}", file.display());
        }
    }
    Ok(())
}
