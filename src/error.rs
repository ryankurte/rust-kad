
#[derive(PartialEq, Clone, Debug)]
pub enum Error {
    Unimplemented,
    Io(std::io::ErrorKind),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e.kind())
    }
}