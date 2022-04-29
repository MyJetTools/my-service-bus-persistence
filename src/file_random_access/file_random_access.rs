use super::{FileRandomAccessEngine, FileRandomAccessInMem};

pub enum FileRandomAccess {
    AsFile(FileRandomAccessEngine),
    InMem(FileRandomAccessInMem),
}

impl FileRandomAccess {
    pub fn create_as_in_mem() -> Self {
        Self::InMem(FileRandomAccessInMem::new())
    }

    pub async fn opend_and_append_as_file(file_name: &str) -> std::io::Result<Self> {
        let as_file = FileRandomAccessEngine::opend_and_append(file_name).await?;
        Ok(Self::AsFile(as_file))
    }

    pub async fn create_new_file(file_name: &str) -> std::io::Result<Self> {
        let as_file = FileRandomAccessEngine::create(file_name).await?;
        Ok(Self::AsFile(as_file))
    }

    pub async fn write_to_file(&mut self, content: &[u8]) -> std::io::Result<()> {
        match self {
            FileRandomAccess::AsFile(as_file) => as_file.write_to_file(content).await,
            FileRandomAccess::InMem(as_mem) => as_mem.write_to_file(content),
        }
    }

    pub async fn reduce_size(&mut self, len: usize) -> std::io::Result<()> {
        match self {
            FileRandomAccess::AsFile(as_file) => as_file.reduce_size(len).await,
            FileRandomAccess::InMem(as_mem) => as_mem.reduce_size(len),
        }
    }

    pub async fn read_from_file(&mut self, dest: &mut [u8]) -> std::io::Result<()> {
        match self {
            FileRandomAccess::AsFile(as_file) => as_file.read_from_file(dest).await,
            FileRandomAccess::InMem(as_mem) => as_mem.read_from_file(dest),
        }
    }

    pub async fn set_position(&mut self, position: usize) -> std::io::Result<()> {
        match self {
            FileRandomAccess::AsFile(as_file) => as_file.set_position(position).await,
            FileRandomAccess::InMem(as_mem) => as_mem.set_position(position),
        }
    }

    pub async fn get_file_size(&mut self) -> std::io::Result<usize> {
        match self {
            FileRandomAccess::AsFile(as_file) => as_file.get_file_size().await,
            FileRandomAccess::InMem(as_mem) => as_mem.get_file_size(),
        }
    }

    #[cfg(test)]
    pub fn unwrap_as_mem(&self) -> &FileRandomAccessInMem {
        match self {
            FileRandomAccess::AsFile(_) => {
                panic!("File is created as read file")
            }
            FileRandomAccess::InMem(as_mem) => &as_mem,
        }
    }
}
