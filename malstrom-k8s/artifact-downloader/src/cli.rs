use std::{fmt::Display, path::PathBuf};

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum StorageType {
    /// Unknown kind, try to derive from other attributes
    Unknown,
    /// S3 endpoint, by default from AWS
    S3,
    /// Google Cloud Platform
    Gcp,
    /// Azure Storage
    Azure,
    /// Http(s) endpoint
    Http,
}

impl Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let matched = match self {
            StorageType::Unknown => "unknown",
            StorageType::S3 => "s3",
            StorageType::Gcp => "gcp",
            StorageType::Azure => "azure",
            StorageType::Http => "http",
        };
        f.write_str(matched)
    }
}

/// Downloader for Malstrom artifact from a variety of sources.
#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ArtifactDownloader {
    /// Full URI to the source executable
    #[arg(short, long)]
    pub source: String,

    /// Local path including the file name
    #[arg(short, long)]
    pub destination: PathBuf,

    /// Source type of download endpoint
    #[arg(short, long, default_value_t = StorageType::Unknown)]
    pub storage_type: StorageType,
}
