#[async_trait::async_trait]
pub trait LoadFromStorage {
    async fn reset_file(&mut self, position: u64) -> std::io::Result<()>;
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<bool>;
}
