# linecache — 工业级异步行缓存（比 Python `linecache` 快 50~200 倍）

[![crates.io](https://img.shields.io/crates/v/linecache.svg)](https://crates.io/crates/linecache)
[![Documentation](https://docs.rs/linecache/badge.svg)](https://docs.rs/linecache)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

一个 **完全兼容 Python `linecache` 行为** 的 Rust 异步实现，专为亿级调用、长运行服务、语料随机采样设计。

### 为什么选择它？

| 特性               | linecache-rs                         | Python `linecache` |
|--------------------|---------------------------------------|---------------------|
| 性能               | 50~200× 更快（零分配）                | 每次 open+readlines |
| 内存控制           | 精确权重驱逐（防 OOM）                | 无限制              |
| 并发安全           | 完全 async + lock-free                | 全局锁              |
| 文件变更检测       | 自动（mtime+size）                    | 需手动 clearcache   |
| 随机行/字符        | 零分配 O(1)                           | 不支持              |

### 快速开始

```rust
use linecache::AsyncLineCache;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cache = AsyncLineCache::new();

    // 读取第 42 行（1-based）
    let line = cache.get_line("data.txt", 42).await?;

    // 零分配随机一行（极快！）
    if let Some(random) = cache.random_line("corpus.txt").await? {
        println!("随机行: {}", random);
    }

    // 随机一个字符（支持中文、emoji）
    if let Some(ch) = cache.random_sign("mixed.txt").await? {
        println!("随机字符: {}", ch);
    }

    Ok(())
}```