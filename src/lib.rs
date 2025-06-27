use dashmap::DashMap;  // 线程安全的并发HashMap实现
use std::fs;
use std::io;
use std::sync::Arc;    // 原子引用计数指针
use tokio::fs::File;   // 异步文件操作
use tokio::io::AsyncBufReadExt;  // 异步缓冲读取特性
use rand::seq::SliceRandom;      // 随机选择功能
use std::time::SystemTime;      // 系统时间

/// 异步行缓存结构体
/// 提供对文件内容的高效缓存访问，支持并发读取
#[derive(Debug, Clone)]
pub struct AsyncLineCache {
    /// 缓存单个行内容（键格式："文件名:行号"）
    pub cache: Arc<DashMap<String, String>>,
    
    /// 记录文件的最后修改时间
    pub file_times: Arc<DashMap<String, SystemTime>>,
    
    /// 缓存文件的所有行（按行存储）
    pub file_lines: Arc<DashMap<String, Vec<String>>>,
    
    /// 缓存文件的完整内容（合并为单个字符串）
    pub file_contents: Arc<DashMap<String, String>>,
}

impl AsyncLineCache {
    /// 创建新的缓存实例
    pub fn new() -> Self {
        AsyncLineCache { 
            cache: Arc::new(DashMap::new()),
            file_times: Arc::new(DashMap::new()),
            file_lines: Arc::new(DashMap::new()),
            file_contents: Arc::new(DashMap::new()),
        }
    }

    /// 获取指定文件的特定行
    /// # 参数
    /// - filename: 文件名
    /// - lineno: 行号（从1开始）
    /// # 返回
    /// - Ok(Some(String)): 找到的行内容
    /// - Ok(None): 行不存在
    /// - Err: 文件操作错误
    pub async fn get_line(&self, filename: &str, lineno: usize) -> io::Result<Option<String>> {
        // 检查文件是否已修改
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            self.check_cache(filename).await?;
        }
        
        // 先检查行缓存
        let key = format!("{}:{}", filename, lineno);
        if let Some(line) = self.cache.get(&key) {
            return Ok(Some(line.value().clone()));
        }
        
        // 从行列表中获取
        if let Some(lines) = self.file_lines.get(filename) {
            if lineno <= lines.len() {
                let line = lines[lineno - 1].clone();
                self.cache.insert(key, line.clone());  // 缓存该行
                return Ok(Some(line));
            }
        }
        
        Ok(None)
    }

    /// 获取文件的所有行
    /// # 参数
    /// - filename: 文件名
    /// # 返回
    /// - Ok(Some(Vec<String>)): 文件的所有行
    /// - Ok(None): 文件不存在或为空
    /// - Err: 文件操作错误
    pub async fn get_lines(&self, filename: &str) -> io::Result<Option<Vec<String>>> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            self.check_cache(filename).await?;
        }
        
        if let Some(lines) = self.file_lines.get(filename) {
            return Ok(Some(lines.value().clone()));
        }
        
        Ok(None)
    }

    /// 获取文件的完整内容
    /// # 参数
    /// - filename: 文件名
    /// # 返回
    /// - Ok(Some(String)): 文件的完整内容
    /// - Ok(None): 文件不存在或为空
    /// - Err: 文件操作错误
    pub async fn get_content(&self, filename: &str) -> io::Result<Option<String>> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            self.check_cache(filename).await?;
        }
        
        // 检查内容缓存
        if let Some(content) = self.file_contents.get(filename) {
            return Ok(Some(content.value().clone()));
        }
        
        // 从行列表构建内容
        if let Some(lines) = self.file_lines.get(filename) {
            let content = lines.join("\n");
            self.file_contents.insert(filename.to_string(), content.clone());
            return Ok(Some(content));
        }
        
        Ok(None)
    }

    /// 随机获取文件中的一行
    /// # 参数
    /// - filename: 文件名
    /// # 返回
    /// - Ok(Some(String)): 随机行内容
    /// - Ok(None): 文件为空
    /// - Err: 文件操作错误
    pub async fn random_line(&self, filename: &str) -> io::Result<Option<String>> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        
        let cached_time = self.file_times.get(filename).map(|entry| *entry.value());
        if cached_time.is_none() || cached_time.unwrap() < last_modified {
            self.check_cache(filename).await?;
        }
        
        if let Some(lines) = self.file_lines.get(filename) {
            if !lines.is_empty() {
                let random_line = lines.choose(&mut rand::thread_rng()).cloned();
                return Ok(random_line);
            }
        }
        
        Ok(None)
    }

    /// 随机获取文件中的一个字符
    /// # 参数
    /// - filename: 文件名
    /// # 返回
    /// - Ok(Some(String)): 随机字符
    /// - Ok(None): 文件为空
    /// - Err: 文件操作错误
    pub async fn random_sign(&self, filename: &str) -> io::Result<Option<String>> {
        let line = self.random_line(filename).await?;
        Ok(line.and_then(|l| {
            l.chars()
                .collect::<Vec<_>>()
                .choose(&mut rand::thread_rng())
                .map(|c| c.to_string())
        }))
    }

    /// 清空所有缓存
    pub async fn clear_cache(&self) {
        self.cache.clear();
        self.file_times.clear();
        self.file_lines.clear();
        self.file_contents.clear();
    }

    /// 检查并更新缓存（内部方法）
    async fn check_cache(&self, filename: &str) -> io::Result<()> {
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        
        if let Some(cached_time) = self.file_times.get(filename).map(|entry| *entry.value()) {
            // 文件已修改，重新加载
            if cached_time < last_modified {
                self.reload_file(filename).await?;
            }
        } else {
            // 文件未缓存，首次加载
            self.load_file(filename).await?;
        }
        
        Ok(())
    }

    /// 加载文件到缓存（内部方法）
    async fn load_file(&self, filename: &str) -> io::Result<()> {
        // 异步打开文件
        let file = File::open(filename).await?;
        let reader = tokio::io::BufReader::new(file);
        let mut lines = Vec::new();
        let mut lines_stream = reader.lines();
        
        // 逐行读取
        while let Some(line) = lines_stream.next_line().await? {
            lines.push(line);
        }
        
        // 更新缓存
        self.file_lines.insert(filename.to_string(), lines.clone());
        self.file_contents.insert(filename.to_string(), lines.join("\n"));
        
        // 记录修改时间
        let file_metadata = fs::metadata(filename)?;
        let last_modified = file_metadata.modified()?;
        self.file_times.insert(filename.to_string(), last_modified);
        
        Ok(())
    }

    /// 重新加载文件（内部方法）
    async fn reload_file(&self, filename: &str) -> io::Result<()> {
        // 清除旧缓存
        self.file_lines.remove(filename);
        self.file_contents.remove(filename);
        // 重新加载
        self.load_file(filename).await
    }
}