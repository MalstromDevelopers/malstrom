use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt};

use clap::Parser;
use object_store::ObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder};
use thiserror::Error;
use tracing::{debug, info, warn};
use url::Url;

mod cli;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    match main_inner().await {
        Ok(_) => (),
        Err(e) => {
            panic!("{:?}", eyre::Report::new(e))
        }
    }
}

async fn main_inner() -> Result<(), Error> {
    let args = cli::ArtifactDownloader::parse();
    debug!("Got the following args: {args:?}");

    let kind = match &args.storage_type {
        cli::StorageType::Unknown => {
            warn!("Download kind is Unkown. Try to extract kind from endpoint");
            match &args.source {
                s if s.starts_with("s3") => cli::StorageType::S3,
                s if s.starts_with("gs") => cli::StorageType::Gcp,
                s if s.starts_with("abfs")
                    || s.starts_with("az")
                    || s.starts_with("adl")
                    || s.contains(".dfs.core.windows.net")
                    || s.contains(".blob.core.windows.net")
                    || s.contains(".dfs.fabric.microsoft.com")
                    || s.contains(".blob.fabric.microsoft.com") =>
                {
                    cli::StorageType::Azure
                }
                s if s.starts_with("http") => cli::StorageType::Http,
                _ => return Err(Error::UnableStorageType),
            }
        }
        _ => args.storage_type.clone(),
    };
    info!("Found download kind: {kind:?}");

    let artifact = match kind {
        cli::StorageType::Http => {
            let url = args
                .source
                .parse::<Url>()
                .map_err(|e| Error::InvalidSourceUri(args.source.clone(), e))?;
            // in theory we could use object_store HTTP here, but in practice object_store somehow
            // messes with the URL and i spent 2h trying to figure out the "invalid scheme" before
            // giving up
            reqwest::get(&url.to_string()).await?.bytes().await?
        }
        cli::StorageType::Azure => {
            let path = args.source;
            let path = Path::parse(&path).map_err(|e| Error::InvalidPath(path.to_owned(), e))?;

            MicrosoftAzureBuilder::from_env()
                .build()?
                .get(&path)
                .await?
                .bytes()
                .await?
        }
        cli::StorageType::Gcp => {
            let path = args.source;
            let path = Path::parse(&path).map_err(|e| Error::InvalidPath(path.to_owned(), e))?;

            GoogleCloudStorageBuilder::from_env()
                .build()?
                .get(&path)
                .await?
                .bytes()
                .await?
        }
        cli::StorageType::S3 => {
            let path = args.source;
            let path = Path::parse(&path).map_err(|e| Error::InvalidPath(path.to_owned(), e))?;
            AmazonS3Builder::from_env()
                .build()?
                .get(&path)
                .await?
                .bytes()
                .await?
        }
        cli::StorageType::Unknown => unreachable!(),
    };

    let path = args.destination;
    let parent = path.parent().ok_or(Error::NoParent)?;
    std::fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new()
        .mode(0o755)
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.write_all(&artifact)?;

    info!("Successfully downloaded artifact");

    Ok(())
}

#[derive(Error, Debug)]
enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Given destination path has no parent directory")]
    NoParent,
    #[error("Unable to determine storage type")]
    UnableStorageType,
    #[error("ObjectStore error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("Invalid source URI: {0} {1}")]
    InvalidSourceUri(String, url::ParseError),
    #[error("Invalid path: {0}, {1}")]
    InvalidPath(String, object_store::path::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}
