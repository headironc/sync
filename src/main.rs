use actix_web::{middleware, web, App, HttpServer};
use mongodb::{options::ClientOptions, Client};
use r2d2_sqlite::SqliteConnectionManager;

use sync::*;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // connect to sqlite database
    let current_dir = std::env::current_dir().unwrap();
    let sqlite_manager = SqliteConnectionManager::file(current_dir.join("job-queue.db"));
    let sqlite_pool = sqlite::Pool::new(sqlite_manager).unwrap();

    // connect to mongodb database
    let mongodb_uri =
        std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    let client_options = ClientOptions::parse(mongodb_uri).await.unwrap();
    let mongodb_client = Client::with_options(client_options).unwrap();

    // create index
    mongo::create_task_id_index(&mongodb_client).await;

    let port = std::env::var("port")
        .unwrap_or_else(|_| "4000".to_string())
        .parse::<u16>()
        .unwrap();

    log::info!("The app is running at http://127.0.0.1:{}", port);

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::Data::new(sqlite_pool.clone()))
            .app_data(web::Data::new(mongodb_client.clone()))
            .service(
                web::resource("/task")
                    .route(web::get().to(task::get_task))
                    .route(web::patch().to(task::update_task)),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
