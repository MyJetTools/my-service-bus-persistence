use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct FileRandomAccessEngine {
    file: tokio::fs::File,
}

impl FileRandomAccessEngine {
    pub async fn opend_and_append(file_name: &str) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_name)
            .await?;

        let result = Self { file };

        Ok(result)
    }

    pub async fn create(file_name: &str) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .open(file_name)
            .await?;

        let result = Self { file };

        Ok(result)
    }

    pub async fn set_position(&mut self, pos: usize) -> std::io::Result<()> {
        self.file.seek(std::io::SeekFrom::Start(pos as u64)).await?;
        Ok(())
    }

    pub async fn write_to_file(&mut self, content: &[u8]) -> std::io::Result<()> {
        self.file.write_all(content).await?;

        Ok(())
    }

    pub async fn read_from_file(&mut self, dest: &mut [u8]) -> std::io::Result<()> {
        let read = self.file.read(dest).await?;

        Ok(())
    }

    pub async fn reduce_size(&mut self, len: usize) -> std::io::Result<()> {
        self.file.set_len(len as u64).await?;

        Ok(())
    }
}
