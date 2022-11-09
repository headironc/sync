use actix_web::web;
use mongodb::{bson::doc, error::Result, options::IndexOptions, Client, Collection, IndexModel};

use crate::task::Task;

pub enum Queries {
    InsertTasks(Vec<Task>),
    UpdateTask(Task),
    GetTask,
}

pub async fn execute(client: &Client, query: Queries) -> Result<Option<Task>> {
    let client = client.clone();

    let collection = web::block(move || client.database("sync").collection::<Task>("tasks"))
        .await
        .unwrap();

    match query {
        Queries::GetTask => get_task(collection).await,
        Queries::InsertTasks(tasks) => insert_tasks(collection, tasks).await,
        Queries::UpdateTask(task) => update_task(collection, task).await,
    }
}

async fn get_task(collection: Collection<Task>) -> Result<Option<Task>> {
    collection
        .find_one(doc! { "run_hostname" : { "$type": 10 }}, None)
        .await
}

async fn insert_tasks(collection: Collection<Task>, tasks: Vec<Task>) -> Result<Option<Task>> {
    let insert_result = collection.insert_many(tasks, None).await;

    match insert_result {
        Ok(_) => get_task(collection).await,
        Err(e) => Err(e),
    }
}

async fn update_task(collection: Collection<Task>, task: Task) -> Result<Option<Task>> {
    println!("Updating task: {:#?}", task);
    let update_result = collection
        .update_one(
            doc! { "task_id": task.task_id },
            doc! {
                "$set":
                {
                    "status": task.status,
                    "typo": task.typo,
                    "percent": task.percent,
                    "max_num_run": task.max_num_run,
                    "ret_val": task.ret_val,
                    "num_run": task.num_run,
                    "workload": task.workload,
                    "run_hostname": task.run_hostname,
                    "run_username": task.run_username,
                    "start_time": task.start_time,
                    "end_time": task.end_time,
                    "last_msg": task.last_msg,
                    "milestone": task.milestone,
                }
            },
            None,
        )
        .await;

    match update_result {
        Ok(_) => get_task(collection).await,
        Err(e) => Err(e),
    }
}

pub async fn create_task_id_index(client: &Client) {
    let options = IndexOptions::builder().unique(true).build();
    let model = IndexModel::builder()
        .keys(doc! { "task_id": 1 })
        .options(options)
        .build();
    client
        .database("sync")
        .collection::<Task>("tasks")
        .create_index(model, None)
        .await
        .expect("creating an index should succeed");
}
