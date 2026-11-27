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

/// Grouping key for a record field: strings use their raw value,
/// other JSON values use their serialization, missing → "null".
fn record_key(r: &Value, field: &str) -> String {
    match r.get(field) {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => "null".into(),
    }
}

/// Apply a transform to a record list.
pub fn apply_step(records: &mut Vec<Value>, step: &Transform) -> Result<(), WrangleError> {
    match step {
        Transform::Filter { field, equals } => {
            records.retain(|r| r.get(field).and_then(|v| v.as_str()) == Some(equals.as_str()));
        }
        Transform::FilterGte { field, value } => {
            records.retain(|r| {
                r.get(field)
                    .and_then(|v| v.as_f64())
                    .map(|x| x >= *value)
                    .unwrap_or(false)
            });
        }
        Transform::FilterLte { field, value } => {
            records.retain(|r| {
                r.get(field)
                    .and_then(|v| v.as_f64())
                    .map(|x| x <= *value)
                    .unwrap_or(false)
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
            records.retain(|r| seen.insert(record_key(r, field)));
        }
        Transform::CountBy { field, out } => {
            let mut counts: BTreeMap<String, u64> = BTreeMap::new();
            for r in records.iter() {
                *counts.entry(record_key(r, field)).or_default() += 1;
            }
            let new_records: Vec<Value> = counts
                .into_iter()
                .map(|(k, c)| serde_json::json!({ field.clone(): k, out.clone(): c }))
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
        _ => Err(WrangleError::Pipeline(
            "input must be array or {records: []}".into(),
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn recs() -> Vec<Value> {
        vec![
            json!({"name": "a", "score": 10, "grp": "x"}),
            json!({"name": "b", "score": 20, "grp": "y"}),
            json!({"name": "c", "score": 30, "grp": "x"}),
        ]
    }

    #[test]
    fn filter_variants() {
        let mut r = recs();
        apply_step(
            &mut r,
            &Transform::Filter {
                field: "grp".into(),
                equals: "x".into(),
            },
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        apply_step(
            &mut r,
            &Transform::FilterGte {
                field: "score".into(),
                value: 15.0,
            },
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["name"], "c");
        apply_step(
            &mut r,
            &Transform::FilterLte {
                field: "score".into(),
                value: 5.0,
            },
        )
        .unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn field_ops() {
        let mut r = recs();
        apply_step(
            &mut r,
            &Transform::Set {
                field: "tag".into(),
                value: json!("t"),
            },
        )
        .unwrap();
        assert_eq!(r[0]["tag"], "t");
        apply_step(
            &mut r,
            &Transform::Copy {
                from: "name".into(),
                to: "nm".into(),
            },
        )
        .unwrap();
        assert_eq!(r[1]["nm"], "b");
        apply_step(
            &mut r,
            &Transform::Rename {
                from: "grp".into(),
                to: "group".into(),
            },
        )
        .unwrap();
        assert!(r[0].get("grp").is_none());
        assert_eq!(r[0]["group"], "x");
        apply_step(
            &mut r,
            &Transform::Drop {
                field: "score".into(),
            },
        )
        .unwrap();
        assert!(r.iter().all(|v| v.get("score").is_none()));
    }

    #[test]
    fn sort_limit_dedup() {
        let mut r = recs();
        apply_step(
            &mut r,
            &Transform::Sort {
                field: "score".into(),
            },
        )
        .unwrap();
        assert_eq!(r[0]["name"], "a");
        assert_eq!(r[2]["name"], "c");
        apply_step(&mut r, &Transform::Limit { n: 2 }).unwrap();
        assert_eq!(r.len(), 2);
        let mut d = vec![json!({"k": 1}), json!({"k": 1}), json!({"k": 2})];
        apply_step(&mut d, &Transform::Dedup { field: "k".into() }).unwrap();
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn aggregates() {
        let mut r = recs();
        apply_step(
            &mut r,
            &Transform::CountBy {
                field: "grp".into(),
                out: "n".into(),
            },
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        let x = r.iter().find(|v| v["grp"] == "x").unwrap();
        assert_eq!(x["n"], 2);
        let mut s = recs();
        apply_step(
            &mut s,
            &Transform::Sum {
                field: "score".into(),
                out: "total".into(),
            },
        )
        .unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0]["total"], 60.0);
    }

    #[test]
    fn load_records_variants() {
        let dir = std::env::temp_dir().join(format!("wrangle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let arr = dir.join("a.json");
        std::fs::write(&arr, r#"[{"x":1},{"x":2}]"#).unwrap();
        assert_eq!(load_records(&arr).unwrap().len(), 2);
        let obj = dir.join("o.json");
        std::fs::write(&obj, r#"{"records":[{"x":9}]}"#).unwrap();
        assert_eq!(load_records(&obj).unwrap()[0]["x"], 9);
        let bad = dir.join("b.json");
        std::fs::write(&bad, r#"{"nope":[]}"#).unwrap();
        assert!(load_records(&bad).is_err());
        assert!(load_records(&dir.join("missing.json")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_pipeline_end_to_end() {
        let dir = std::env::temp_dir().join(format!("wrangle-run-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let inp = dir.join("in.json");
        let outp = dir.join("out.json");
        std::fs::write(&inp, r#"[{"k":"a","v":1},{"k":"b","v":2},{"k":"a","v":3}]"#).unwrap();
        let p = Pipeline {
            name: "t".into(),
            input: inp,
            output: outp.clone(),
            steps: vec![
                Transform::Filter {
                    field: "k".into(),
                    equals: "a".into(),
                },
                Transform::Sum {
                    field: "v".into(),
                    out: "total".into(),
                },
            ],
        };
        let res = run(&p).unwrap();
        assert_eq!(res[0]["total"], 4.0);
        assert!(outp.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pipeline_serde_roundtrip() {
        let p = Pipeline {
            name: "n".into(),
            input: PathBuf::from("/in.json"),
            output: PathBuf::from("/out.json"),
            steps: vec![Transform::Limit { n: 5 }],
        };
        let j = serde_json::to_string(&p).unwrap();
        let back: Pipeline = serde_json::from_str(&j).unwrap();
        assert_eq!(back.name, "n");
        assert_eq!(back.steps.len(), 1);
        // tagged enum: op field present
        assert!(j.contains("\"op\":\"limit\""));
    }
}
