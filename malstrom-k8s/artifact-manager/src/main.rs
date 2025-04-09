use std::path::PathBuf;

use rocket::{
    form::Form,
    fs::{NamedFile, TempFile},
    get,
    http::Status,
    launch, post, routes, FromForm,
};
use tracing::{error, info};

// Rocket configuration
#[launch]
fn rocket() -> _ {
    tracing_subscriber::fmt::init();
    rocket::build().mount("/", routes![upload, serve_file, health])
}

#[derive(FromForm)]
struct Upload<'f> {
    pub file: TempFile<'f>,
}

#[post("/<name>", format = "multipart/form-data", data = "<form>")]
async fn upload(mut form: Form<Upload<'_>>, name: &str) -> Result<Status, Status> {
    // Directory where files will be saved
    let upload_dir = "./artifacts/";

    // Ensure the directory exists
    if let Err(e) = std::fs::create_dir_all(upload_dir) {
        error!("Failed to create upload directory: {}", e);
        return Err(Status::InternalServerError);
    }

    // File path where we will save the uploaded file
    let file_path = PathBuf::from(upload_dir).join(name); // Adjust the name logic as needed

    // Stream data into the file
    if let Err(e) = form.file.move_copy_to(&file_path).await {
        error!("Failed to write file: {}", e);
        return Err(Status::InternalServerError);
    }

    info!("File uploaded successfully to {:?}", file_path);
    Ok(Status::Ok)
}

/// Endpoint to serve uploaded files (optional)
#[get("/<filename>")]
async fn serve_file(filename: &str) -> Option<NamedFile> {
    let path = PathBuf::from("./artifacts/").join(filename);
    NamedFile::open(path).await.ok()
}

/// Health check endpoint
#[get("/health")]
async fn health() -> Status {
    Status::Ok
}
