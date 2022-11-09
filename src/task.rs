use actix_web::{web, HttpResponse};
use mongodb::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Task {
    pub job_name: String,
    pub task_id: String,
    pub status: u32,
    pub typo: u32,
    pub percent: f32,
    pub max_num_run: u32,
    pub ret_val: Option<i32>,
    pub num_run: u32,
    pub workload: f32,
    pub run_hostname: Option<String>,
    pub run_username: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub last_msg: Option<String>,
    pub milestone: Option<String>,
}

pub async fn get_task(
    client: web::Data<Client>,
    sqlite_pool: web::Data<sqlite::Pool>,
) -> HttpResponse {
    let task_result = mongo::execute(&client, mongo::Queries::GetTask).await;

    match task_result {
        Ok(Some(task)) => HttpResponse::Ok().json(json!(task)),
        Ok(None) => {
            log::info!("No incomplete task found in MongoDB, trying SQLite");
            let tasks = sqlite::execute(&sqlite_pool, sqlite::Queries::GetAllTasks)
                .await
                .unwrap();
            let result = mongo::execute(&client, mongo::Queries::InsertTasks(tasks.clone())).await;

            match result {
                Ok(Some(task)) => HttpResponse::Ok().json(json!(task)),
                Ok(None) => HttpResponse::NotFound().json(json!({"message": "No task found"})),
                Err(e) => {
                    HttpResponse::InternalServerError().json(json!({"message":e.to_string()}))
                }
            }
        }
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
    }
}

pub async fn update_task(
    client: web::Data<Client>,
    sqlite_pool: web::Data<sqlite::Pool>,
    task: web::Json<Task>,
) -> HttpResponse {
    let result = mongo::execute(&client, mongo::Queries::UpdateTask(task.clone())).await;

    match result {
        Ok(Some(task)) => {
            println!("task: {:#?}", task);
            HttpResponse::Ok().json(json!(task)) },
        Ok(None) => {
            log::info!("No incomplete task found in MongoDB, trying SQLite");
            let tasks = sqlite::execute(&sqlite_pool, sqlite::Queries::GetAllTasks)
                .await
                .unwrap();

            let result = mongo::execute(&client, mongo::Queries::InsertTasks(tasks.clone())).await;

            match result {
                Ok(Some(task)) => HttpResponse::Ok().json(json!(task)),
                Ok(None) => HttpResponse::NotFound().json(json!({"message": "No task found"})),
                Err(e) => {
                    HttpResponse::InternalServerError().json(json!({"message":e.to_string()}))
                }
            }
        }
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
    }
}
