// Author: Torin Etheridge
// Date: 2026-10-04

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Ready,
    Leased,
    Succeeded,
    Retrying,
    Dead,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Dead)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_value(self)
                .expect("enum serializes")
                .as_str()
                .expect("enum is string")
        )
    }
}

impl FromStr for JobState {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{value}\""))
            .map_err(|_| format!("invalid job state: {value}"))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

impl Priority {
    pub fn value(self) -> i64 {
        match self {
            Self::Low => 0,
            Self::Normal => 1,
            Self::High => 2,
            Self::Critical => 3,
        }
    }
    pub fn from_value(value: i64) -> Self {
        match value {
            0 => Self::Low,
            2 => Self::High,
            3 => Self::Critical,
            _ => Self::Normal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryPolicy {
    Fixed {
        delay_ms: u64,
    },
    Exponential {
        base_ms: u64,
        max_delay_ms: u64,
        jitter: bool,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::Exponential {
            base_ms: 1_000,
            max_delay_ms: 60_000,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    pub fn delay_ms(&self, attempts: u32, random_unit: f64) -> u64 {
        match *self {
            Self::Fixed { delay_ms } => delay_ms,
            Self::Exponential {
                base_ms,
                max_delay_ms,
                jitter,
            } => {
                let exponent = attempts.saturating_sub(1).min(62);
                let bounded = base_ms.saturating_mul(1_u64 << exponent).min(max_delay_ms);
                if jitter {
                    ((bounded as f64) * (0.5 + random_unit.clamp(0.0, 1.0) * 0.5)) as u64
                } else {
                    bounded
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJob {
    pub queue: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default = "default_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub delay_ms: u64,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "default_dedup_ms")]
    pub dedup_ms: u64,
    #[serde(default)]
    pub retry_policy: RetryPolicy,
}

fn default_attempts() -> u32 {
    3
}
fn default_dedup_ms() -> u64 {
    86_400_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub payload: serde_json::Value,
    pub priority: Priority,
    pub state: JobState,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at_ms: i64,
    pub available_at_ms: i64,
    pub lease_owner: Option<String>,
    #[serde(skip_serializing)]
    pub lease_token: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub retry_policy: RetryPolicy,
}

pub fn valid_queue_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exponential_backoff_is_bounded() {
        let p = RetryPolicy::Exponential {
            base_ms: 1000,
            max_delay_ms: 8000,
            jitter: false,
        };
        assert_eq!(
            (1..=6).map(|n| p.delay_ms(n, 0.0)).collect::<Vec<_>>(),
            [1000, 2000, 4000, 8000, 8000, 8000]
        );
    }
    proptest::proptest! {
        #[test]
        fn jitter_never_exceeds_bounds(attempt in 1u32..100, unit in 0f64..1f64) {
            let p = RetryPolicy::Exponential { base_ms: 1000, max_delay_ms: 8000, jitter: true };
            let delay = p.delay_ms(attempt, unit);
            proptest::prop_assert!(delay <= 8000);
            proptest::prop_assert!(delay >= 500);
        }
    }
}
