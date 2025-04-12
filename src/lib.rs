use dashmap::DashMap;
use std::fs;
use std::io;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use rand::seq::SliceRandom; // 导入 SliceRandom trait
use std::time::SystemTime;

pub struct AsyncLineCache {
    pub cache: Arc<DashMap<String, String>>, // 缓存文件行内容
    pub file_times: Arc<DashMap<String, SystemTime>>, // 缓存文件的最后修改时间
    pub file_lines: Arc<DashMap<String, Vec<String>>>, // 缓存文件的所有行
}

impl AsyncLineCache {
    pub fn new() -> Self {
        let cache = Arc::new(DashMap::new()); // 使用 DashMap 缓存文件行内容
        let file_times = Arc::new(DashMap::new()); // 使用 DashMap 缓存文件的最后修改时间
        let file_lines = Arc::new(DashMap::new()); // 使用 DashMap 缓存文件的所有行
        AsyncLineCache { cache, file_times, file_lines }
    }

    pub async fn get_line(&self, filename: &str, lineno: usize) -> io::Result<Option<String>> {
        // println!("正在从文件 {} 中获取第 {} 行...", filename, lineno);
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        // 检查文件是否更新
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            // println!("文件已更新，正在重新加载缓存...");
            self.check_cache(filename).await?;
        }
        // println!("缓存检查完成...");
        // 生成缓存键：文件名 + 行号
        let key = format!("{}:{}", filename, lineno);
        if let Some(line) = self.cache.get(&key) {
            // println!("从缓存中找到行内容: {:?}", line);
            return Ok(Some(line.value().clone()));
        }
        // 如果缓存中没有，从文件中读取
        if let Some(lines) = self.file_lines.get(filename) {
            if lineno <= lines.len() {
                let line = lines[lineno - 1].clone();
                self.cache.insert(key, line.clone());
                // println!("从文件中读取行内容: {}", line);
                return Ok(Some(line));
            }
        }
        // println!("文件 {} 中未找到第 {} 行", filename, lineno);
        Ok(None)
    }

    pub async fn get_lines(&self, filename: &str) -> io::Result<Option<Vec<String>>> {
        // println!("正在从文件 {} 中获取所有行...", filename);
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        // 检查文件是否更新
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            // println!("文件已更新，正在重新加载缓存...");
            self.check_cache(filename).await?;
        }
        // println!("缓存检查完成...");
        // 从缓存中获取所有行
        if let Some(lines) = self.file_lines.get(filename) {
            // println!("成功获取文件 {} 的所有行", filename);
            return Ok(Some(lines.value().clone()));
        }
        // println!("文件 {} 未找到或为空", filename);
        Ok(None)
    }

    pub async fn random_line(&self, filename: &str) -> io::Result<Option<String>> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        // 检查文件是否更新
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            self.check_cache(filename).await?;
        }
        // 从缓存中获取所有行并随机选择一行
        if let Some(lines) = self.file_lines.get(filename) {
            if !lines.is_empty() {
                let random_line = lines.choose(&mut rand::thread_rng()).cloned();
                return Ok(random_line);
            }
        }
        Ok(None)
    }

    pub async fn clear_cache(&self) {
        // 清空行内容缓存
        self.cache.clear();
        // 清空文件修改时间缓存
        self.file_times.clear();
        // 清空文件行缓存
        self.file_lines.clear();
    }

    async fn check_cache(&self, filename: &str) -> io::Result<()> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        if let Some(cached_time) = self.file_times.get(filename).map(|entry| *entry.value()) {
            if cached_time < last_modified {
                // println!("文件已更新，正在重新加载...");
                self.reload_file(filename).await?;
            }
        } else {
            // println!("文件未缓存，正在加载...");
            self.load_file(filename).await?;
        }
        Ok(())
    }

    async fn load_file(&self, filename: &str) -> io::Result<()> {
        let file = File::open(filename).await?;
        let reader = tokio::io::BufReader::new(file);
        let mut lines = Vec::new();
        let mut lines_stream = reader.lines();
        // 逐行读取文件内容
        while let Some(line) = lines_stream.next_line().await? {
            lines.push(line);
        }
        self.file_lines.insert(filename.to_string(), lines);
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        self.file_times.insert(filename.to_string(), last_modified);
        Ok(())
    }

    async fn reload_file(&self, filename: &str) -> io::Result<()> {
        self.file_lines.remove(filename);
        self.load_file(filename).await
    }
}