use linecache::AsyncLineCache;
use std::{collections::HashSet, time::Duration};
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_basic_line_retrieval_and_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();
    let content = "Line 1\nLine 2\nLine 3\nLast Line\n";
    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();
    std::fs::write(&path, content)?;

    assert_eq!(cache.get_line(&path, 1).await?.unwrap(), "Line 1");
    assert_eq!(cache.get_line(&path, 4).await?.unwrap(), "Last Line");
    assert_eq!(cache.get_line(&path, 5).await?.unwrap(), ""); // 尾随空行
    assert_eq!(cache.get_line(&path, 6).await?, None);
    assert_eq!(cache.get_line(&path, 0).await?, None);

    Ok(())
}

#[tokio::test]
async fn test_empty_and_not_found_files() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();

    // 1. 文件不存在
    assert_eq!(
        cache.get_content("not-exist.txt").await?,
        Some("".to_string())
    );

    // 2. 空文件
    let empty = NamedTempFile::new()?;
    let ep = empty.path().to_str().unwrap().to_string();
    std::fs::write(&ep, "")?;
    assert_eq!(cache.get_lines(&ep).await?, vec![] as Vec<String>);
    assert_eq!(cache.get_content(&ep).await?, Some("".to_string()));

    // 3. 只包含一个 \n 的文件 —— 真实 Python linecache 行为
    let nl = NamedTempFile::new()?;
    let np = nl.path().to_str().unwrap().to_string();
    std::fs::write(&np, "\n")?;

    assert_eq!(cache.get_line(&np, 1).await?.unwrap(), "");
    assert_eq!(cache.get_line(&np, 2).await?.unwrap(), "");
    assert_eq!(cache.get_line(&np, 3).await?, None);

    assert_eq!(
        cache.get_lines(&np).await?,
        vec!["".to_string(), "".to_string()]
    );
    assert_eq!(cache.get_content(&np).await?, Some("\n".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_file_modification_detection() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();
    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();

    std::fs::write(&path, "v1\n")?;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(cache.get_line(&path, 1).await?.unwrap(), "v1");
    assert_eq!(cache.get_line(&path, 2).await?.unwrap(), "");

    std::fs::write(&path, "v2\nv22\n")?;
    sleep(Duration::from_millis(100)).await;

    assert_eq!(cache.get_line(&path, 1).await?.unwrap(), "v2");
    assert_eq!(cache.get_line(&path, 2).await?.unwrap(), "v22");
    assert_eq!(cache.get_line(&path, 3).await?.unwrap(), "");

    Ok(())
}

#[tokio::test]
async fn test_get_lines_and_content() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();
    let content = "Hello\nWorld\nRust\n"; // 以 \n 结尾
    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();
    std::fs::write(&path, content)?;

    // 正确！因为文件以 \n 结尾，linecache 必须多一个空行
    let expected_lines = vec!["Hello", "World", "Rust", ""];
    assert_eq!(cache.get_lines(&path).await?, expected_lines);

    // 正确！原始内容就是这样
    let expected_content = "Hello\nWorld\nRust\n";
    assert_eq!(
        cache.get_content(&path).await?,
        Some(expected_content.to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_random_getters() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();
    let content = "A\nB\nC\nD\n中文\n🚀\n";
    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();
    std::fs::write(&path, content)?;

    let mut seen_lines = HashSet::new();
    let mut seen_chars = HashSet::new();

    for _ in 0..200 {
        if let Some(line) = cache.random_line(&path).await? {
            seen_lines.insert(line);
        }
        if let Some(ch) = cache.random_sign_string(&path).await? {
            seen_chars.insert(ch);
        }
    }

    assert!(seen_lines.len() > 3);
    assert!(seen_chars.len() > 5);

    Ok(())
}

#[tokio::test]
async fn test_invalidation_and_clear() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();

    let f1 = NamedTempFile::new()?;
    let p1 = f1.path().to_str().unwrap().to_string();
    std::fs::write(&p1, "file1\n")?;

    let f2 = NamedTempFile::new()?;
    let p2 = f2.path().to_str().unwrap().to_string();
    std::fs::write(&p2, "file2\n")?;

    cache.get_line(&p1, 1).await?;
    cache.get_content(&p1).await?;
    cache.get_line(&p2, 1).await?;

    assert!(cache.lines.get(&p1).await.is_some());
    assert!(cache.contents.get(&p1).await.is_some());

    cache.invalidate(&p1).await;
    assert!(cache.lines.get(&p1).await.is_none());

    assert!(cache.lines.get(&p2).await.is_some());

    cache.clear().await;
    assert!(cache.lines.get(&p2).await.is_none());

    Ok(())
}

#[tokio::test]
async fn test_weigher_sanity() -> Result<(), Box<dyn std::error::Error>> {
    let cache = AsyncLineCache::new();
    let big = "X".repeat(10 * 1024 * 1024);
    let content = format!("{big}\nLine2\n");

    let file = NamedTempFile::new()?;
    let path = file.path().to_str().unwrap().to_string();
    std::fs::write(&path, content)?;

    cache.get_lines(&path).await?;
    cache.get_content(&path).await?;

    assert!(cache.lines.get(&path).await.is_some());
    assert!(cache.contents.get(&path).await.is_some());

    cache.clear().await;
    Ok(())
}
