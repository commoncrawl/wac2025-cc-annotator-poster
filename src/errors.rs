use std::fmt;

#[derive(Debug)]
pub enum AnnotatetorError {
    Tokio(tokio::io::Error),
    Join(tokio::task::JoinError),
    Custom(String),
}

impl fmt::Display for AnnotatetorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AnnotatetorError::Tokio(ref err) => err.fmt(f),
            AnnotatetorError::Join(ref err) => err.fmt(f),
            AnnotatetorError::Custom(ref err) => err.fmt(f),
        }
    }
}

impl From<tokio::io::Error> for AnnotatetorError {
    fn from(err: tokio::io::Error) -> Self {
        AnnotatetorError::Tokio(err)
    }
}

impl From<tokio::task::JoinError> for AnnotatetorError {
    fn from(err: tokio::task::JoinError) -> Self {
        AnnotatetorError::Join(err)
    }
}

impl From<String> for AnnotatetorError {
    fn from(s: String) -> AnnotatetorError {
        AnnotatetorError::Custom(s)
    }
}
