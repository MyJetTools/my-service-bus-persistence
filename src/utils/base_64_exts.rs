pub trait FromBase64 {
    fn from_base64(&self) -> Result<Vec<u8>, base64::DecodeError>;
}

impl FromBase64 for String {
    fn from_base64(&self) -> Result<Vec<u8>, base64::DecodeError> {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.decode(self)
    }
}

pub trait ToBase64 {
    fn to_base64(&self) -> String;
}

impl ToBase64 for Vec<u8> {
    fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self)
    }
}
