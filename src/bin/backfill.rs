use chrono::{DateTime, Local};
use energyhub::connect_sqlite;
use rusqlite::Statement;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::{fmt, io};

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct KamstrupValues {
    pub timestamp: Option<DateTime<Local>>,
    pub energy: f64,
    pub volume: f64,
    temp1: f64,
    temp2: f64,
    pub hourcounter: f64,
}

#[derive(Debug, Clone)]
struct InvalidTariff;
impl Error for InvalidTariff {}
impl fmt::Display for InvalidTariff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid tariff")
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut conn = connect_sqlite().unwrap();

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("INSERT INTO electricity (timestamp, used_t1, used_t2, active_tariff) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")?;
        let file = File::open("dsmr.tsv").unwrap();
        for line in io::BufReader::new(file).lines() {
            let line = line.unwrap();
            let result = dsmr(&mut stmt, &line);
            match result {
                Ok(_) => {}
                Err(e) => {
                    println!("dsmr: error parsing line: {}", line);
                    println!("{}", e);
                }
            }
        }
    }
    tx.commit()?;

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("INSERT INTO heat (timestamp, energy, volume, hourcounter) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")?;
        let file = File::open("kamstrup.ndjson").unwrap();
        for line in io::BufReader::new(file).lines() {
            let line = line.unwrap();
            let result = kamstrup(&mut stmt, &line);
            match result {
                Ok(_) => {}
                Err(e) => {
                    println!("kamstrup: error parsing line: {}", line);
                    println!("{}", e);
                }
            }
        }
    }
    tx.commit()?;

    Ok(())
}

fn dsmr(stmt: &mut Statement, line: &str) -> Result<(), Box<dyn Error>> {
    let mut s = line.trim_matches(char::from(0)).split('\t');
    let timestamp = s.next().unwrap().parse::<DateTime<Local>>()?;
    let timestamp = &timestamp.timestamp();
    let active_tariff = s.next().unwrap().parse::<u8>()?;
    let used_t1 = s.next().unwrap().parse::<f64>()?;
    let used_t1 = (used_t1 * 1e3f64) as u32;
    let used_t2 = s.next().unwrap().parse::<f64>()?;
    let used_t2 = (used_t2 * 1e3f64) as u32;
    if active_tariff != 1 && active_tariff != 2 {
        return Err(InvalidTariff.into());
    }
    stmt.execute((&timestamp, &used_t1, &used_t2, &active_tariff))?;
    Ok(())
}

fn kamstrup(stmt: &mut Statement, line: &str) -> Result<(), Box<dyn Error>> {
    let values: KamstrupValues = serde_json::from_str(line)?;
    let timestamp = match &values.timestamp {
        Some(dt) => dt.timestamp(),
        None => (values.hourcounter as i64 * 3600) + 1454461908,
    };
    let energy = (values.energy * 1e3f64) as u32;
    let volume = (values.volume * 1e3f64) as u32;
    let hourcounter = values.hourcounter as u32;
    stmt.execute((&timestamp, &energy, &volume, &hourcounter))?;
    Ok(())
}
