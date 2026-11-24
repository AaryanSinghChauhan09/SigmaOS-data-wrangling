//! SigmaOS data-wrangling — ETL pipeline engine.
//!
//! Declarative pipelines over JSON records: filter, map, aggregate,
//! sort, limit, dedup. Sources read JSON files/arrays; sinks write JSON.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Errors produced by this component.
#[derive(Debug, thiserror::Error)]
pub enum WrangleError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pipeline error: {0}")]
    Pipeline(String),
    #[error("not found: {0}")]
    NotFound(String),
}

/// A single transform step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Transform {
    /// Keep records where field == value (string compare).
    Filter { field: String, equals: String },
    /// Keep records where numeric field >= value.
    FilterGte { field: String, value: f64 },
    /// Keep records where numeric field <= value.
    FilterLte { field: String, value: f64 },
    /// Add/overwrite a field with a constant value.
    Set { field: String, value: Value },
    /// Copy field -> new field.
    Copy { from: String, to: String },
    /// Drop a field.
    Drop { field: String },
    /// Rename field.
    Rename { from: String, to: String },
    /// Sort by field (ascending; numbers numeric, else lexicographic).
    Sort { field: String },
    /// Keep first N records.
    Limit { n: usize },
    /// Remove duplicate records by field value.
    Dedup { field: String },
    /// Group by field and count records per group.
    CountBy { field: String, out: String },
    /// Sum numeric field into a new field (per record? aggregate) —
    /// here: aggregate total into single record.
    Sum { field: String, out: String },
}

/// A pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    /// Input JSON file (array of objects, or {"records": [...]}).
    pub input: PathBuf,
    /// Output JSON file.
    pub output: PathBuf,
    #[serde(default)]
    pub steps: Vec<Transform>,
}

/// Apply a transform to a record list.
pub fn apply_step(records: &mut Vec<Value>, step: &Transform) -> Result<(), WrangleError> {
    match step {
        Transform::Filter { field, equals } => {
            records.retain(|r| {
                r.get(field).and_then(|v| v.as_str()) == Some(equals.as_str())
            });
        }
        Transform::FilterGte { field, value } => {
            records.retain(|r| {
                r.get(field).and_then(|v| v.as_f64()).map(|x| x >= *value).unwrap_or(false)
            });
        }
        Transform::FilterLte { field, value } => {
            records.retain(|r| {
                r.get(field).and_then(|v| v.as_f64()).map(|x| x <= *value).unwrap_or(false)
            });
        }
        Transform::Set { field, value } => {
            for r in records.iter_mut() {
                if let Some(obj) = r.as_object_mut() {
                    obj.insert(field.clone(), value.clone());
                }
            }
        }
        Transform::Copy { from, to } => {
            for r in records.iter_mut() {
                if let Some(v) = r.get(from).cloned() {
                    if let Some(obj) = r.as_object_mut() {
                        obj.insert(to.clone(), v);
                    }
                }
            }
        }
        Transform::Drop { field } => {
            for r in records.iter_mut() {
                if let Some(obj) = r.as_object_mut() {
                    obj.remove(field);
                }
            }
        }
        Transform::Rename { from, to } => {
            for r in records.iter_mut() {
                if let Some(v) = r.get(from).cloned() {
                    if let Some(obj) = r.as_object_mut() {
                        obj.remove(from);
                        obj.insert(to.clone(), v);
                    }
                }
            }
        }
        Transform::Sort { field } => {
            records.sort_by(|a, b| {
                let av = a.get(field);
                let bv = b.get(field);
                match (av.and_then(|v| v.as_f64()), bv.and_then(|v| v.as_f64())) {
                    (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                    _ => format!("{:?}", av).cmp(&format!("{:?}", bv)),
                }
            });
        }
        Transform::Limit { n } => {
            records.truncate(*n);
        }
        Transform::Dedup { field } => {
            let mut seen = std::collections::BTreeSet::new();
            records.retain(|r| {
                let key = r.get(field).map(|v| v.to_string()).unwrap_or_default();
                seen.insert(key)
            });
        }
        Transform::CountBy { field, out } => {
            let mut counts: BTreeMap<String, u64> = BTreeMap::new();
            for r in records.iter() {
                let key = r.get(field).map(|v| v.to_string()).unwrap_or_else(|| "null".into());
                *counts.entry(key).or_default() += 1;
            }
            let new_records: Vec<Value> = counts
                .into_iter()
                .map(|(k, c)| {
                    serde_json::json!({ field.clone(): k, out.clone(): c })
                })
                .collect();
            *records = new_records;
        }
        Transform::Sum { field, out } => {
            let total: f64 = records
                .iter()
                .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                .sum();
            *records = vec![serde_json::json!({ out.clone(): total })];
        }
    }
    Ok(())
}

/// Load records from a JSON file (array or {"records": [...]}).
pub fn load_records(path: &Path) -> Result<Vec<Value>, WrangleError> {
    let text = std::fs::read_to_string(path)
        .map_err(|_| WrangleError::NotFound(path.display().to_string()))?;
    let v: Value = serde_json::from_str(&text)?;
    match v {
        Value::Array(arr) => Ok(arr),
        Value::Object(obj) => {
            if let Some(Value::Array(arr)) = obj.get("records") {
                Ok(arr.clone())
            } else {
                Err(WrangleError::Pipeline("no records array found".into()))
            }
        }
        _ => Err(WrangleError::Pipeline("input must be array or {records: []}".into())),
    }
}

/// Run a pipeline end-to-end.
pub fn run(pipeline: &Pipeline) -> Result<Vec<Value>, WrangleError> {
    let mut records = load_records(&pipeline.input)?;
    for step in &pipeline.steps {
        apply_step(&mut records, step)?;
    }
    if let Some(parent) = pipeline.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&pipeline.output, serde_json::to_string_pretty(&records)?)?;
    Ok(records)
}

/// Save a pipeline definition as JSON.
pub fn save_pipeline(p: &Pipeline, path: &Path) -> Result<(), WrangleError> {
    std::fs::write(path, serde_json::to_string_pretty(p)?)?;
    Ok(())
}

/// Load a pipeline definition.
pub fn load_pipeline(path: &Path) -> Result<Pipeline, WrangleError> {
    let text = std::fs::read_to_string(path)
        .map_err(|_| WrangleError::NotFound(path.display().to_string()))?;
    Ok(serde_json::from_str(&text)?)
}

/// Default pipelines directory.
pub fn default_pipelines_dir() -> Option<PathBuf> {
    directories::ProjectDirs::from("org", "sigmaos", "sigmaos")
        .map(|p| p.config_dir().join("data-wrangling").join("pipelines"))
}
