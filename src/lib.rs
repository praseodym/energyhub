use chrono::{DateTime, Local};
use rusqlite::{Connection, Result};
use serde::Deserialize;

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct DSMRMeasurements {
    pub timestamp: DateTime<Local>,
    #[serde(rename = "ActiveTariff")]
    pub active_tariff: String,
    #[serde(rename = "ElectricityUsedT1")]
    pub electricity_used_t1: f64,
    #[serde(rename = "ElectricityUsedT2")]
    pub electricity_used_t2: f64,
    #[serde(rename = "CurrentElectricityUsage")]
    pub current_electricity_usage: f64,
    #[serde(rename = "CurrentElectricityDraw")]
    current_electricity_draw: f64,
    #[serde(rename = "InstantaneousActivePowerPositive")]
    instantaneous_active_power_positive: f64,
    #[serde(rename = "InstantaneousActivePowerNegative")]
    instantaneous_active_power_negative: f64,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct KamstrupValues {
    pub timestamp: DateTime<Local>,
    pub energy: f64,
    pub volume: f64,
    temp1: f64,
    temp2: f64,
    pub hourcounter: f64,
}

pub fn connect_sqlite() -> Result<Connection> {
    let conn = Connection::open("energy.sqlite3")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS electricity (
            timestamp      INTEGER PRIMARY KEY,
            used_t1        INTEGER NOT NULL,
            used_t2        INTEGER NOT NULL,
            active_tariff  INTEGER NOT NULL,
            current_usage  INTEGER
        ) STRICT",
        (),
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS heat (
            timestamp      INTEGER PRIMARY KEY,
            energy         INTEGER NOT NULL,
            volume         INTEGER NOT NULL,
            hourcounter    INTEGER NOT NULL
        ) STRICT",
        (),
    )?;
    Ok(conn)
}
