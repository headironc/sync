use actix_web::{error, web, Error};
use rusqlite::Statement;

use crate::task::Task;

pub type Pool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;
pub type Connection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;
type TaskResult = Result<Vec<Task>, rusqlite::Error>;

pub enum Queries {
    GetAllTasks,
}

pub async fn execute(pool: &Pool, query: Queries) -> Result<Vec<Task>, Error> {
    let pool = pool.clone();

    let connection = web::block(move || pool.get())
        .await?
        .map_err(error::ErrorInternalServerError)?;

    web::block(move || match query {
        Queries::GetAllTasks => get_all_tasks(connection),
    })
    .await?
    .map_err(error::ErrorInternalServerError)
}

fn get_all_tasks(connection: Connection) -> TaskResult {
    let statement = connection.prepare("SELECT * FROM tasks")?;

    get_rows_as_task(statement)
}

fn get_rows_as_task(mut statement: Statement) -> TaskResult {
    statement
        .query_map([], |row| {
            Ok(Task {
                job_name: row.get(0)?,
                task_id: row.get(1)?,
                status: row.get(2)?,
                typo: row.get(3)?,
                percent: row.get(4)?,
                max_num_run: row.get(5)?,
                ret_val: row.get(6)?,
                num_run: row.get(7)?,
                workload: row.get(8)?,
                run_hostname: row.get(9)?,
                run_username: row.get(10)?,
                start_time: row.get(11)?,
                end_time: row.get(12)?,
                last_msg: row.get(13)?,
                milestone: row.get(14)?,
            })
        })
        .and_then(Iterator::collect)
}
