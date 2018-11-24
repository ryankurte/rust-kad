/**
 * rust-kad
 * Kademlia error types
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::io::{Error as IoError, ErrorKind as IoErrorKind};

#[derive(PartialEq, Clone, Debug)]
pub enum Error {
    Unimplemented,
    InvalidResponse,
    Timeout,
    Io(IoErrorKind),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        if e.kind() == IoErrorKind::TimedOut {
            Error::Timeout
        } else {
            Error::Io(e.kind())
        }
    }
}