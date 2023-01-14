use axum::{routing::get, Router};
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use energyhub::connect_sqlite;
use rusqlite::{Error, Row};

#[tokio::main]
async fn main() {
    // build our application with a single route
    let app = Router::new().route("/", get(root));

    // run it with hyper on localhost:3000
    // TODO: make listen port dynamic, maybe from env?
    axum::Server::bind(&"[::]:80".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[derive(Debug)]
struct Electricity {
    timestamp: DateTime<Local>,
    used_t1: f64,
    used_t2: f64,
    used_total: f64,
    current_usage: u64,
}

#[derive(Debug)]
struct Heat {
    timestamp: DateTime<Local>,
    energy: f64,
    volume: f64,
    hours: u64,
}

async fn root() -> String {
    let conn = connect_sqlite().unwrap();

    // let res: i64 = conn.query_row("SELECT timestamp, used_t1, used_t2, current_usage FROM electricity ORDER BY timestamp DESC LIMIT 1", (), |row| row.get(0)).unwrap();
    let e: Electricity = conn.query_row("SELECT timestamp, used_t1, used_t2, current_usage FROM electricity ORDER BY timestamp DESC LIMIT 1", (), |row| electricity_row(row)).unwrap();

    let h: Heat = conn
        .query_row(
            "SELECT timestamp, energy,volume,hourcounter FROM heat ORDER BY timestamp DESC LIMIT 1",
            (),
            |row| heat_row(row),
        )
        .unwrap();

    format!("{:?}\n\n{:?}", e, h)
}

fn electricity_row(row: &Row) -> Result<Electricity, Error> {
    // TODO: return rusqlite::Error
    let timestamp = NaiveDateTime::from_timestamp_opt(row.get(0)?, 0).unwrap();
    let timestamp: DateTime<Utc> = DateTime::from_utc(timestamp, Utc);
    let timestamp: DateTime<Local> = DateTime::from(timestamp);

    let mut used_t1 = row.get(1)?;
    let mut used_t2 = row.get(2)?;
    let mut used_total = used_t1 + used_t2;
    used_t1 /= 1e3;
    used_t2 /= 1e3;
    used_total /= 1e3;

    Ok(Electricity {
        timestamp,
        used_t1,
        used_t2,
        used_total,
        current_usage: row.get(3)?,
    })
}

fn heat_row(row: &Row) -> Result<Heat, Error> {
    // TODO: return rusqlite::Error
    let timestamp = NaiveDateTime::from_timestamp_opt(row.get(0)?, 0).unwrap();
    let timestamp: DateTime<Utc> = DateTime::from_utc(timestamp, Utc);
    let timestamp: DateTime<Local> = DateTime::from(timestamp);

    let mut energy = row.get(1)?;
    let mut volume = row.get(2)?;
    energy /= 1e3;
    volume /= 1e3;

    Ok(Heat {
        timestamp,
        energy,
        volume,
        hours: row.get(3)?,
    })
}
