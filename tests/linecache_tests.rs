use linecache::AsyncLineCache;
use std::io::Write;
use tempfile::NamedTempFile;
use tokio::time::{timeout, Duration};
use std::fs;

#[tokio::test]
async fn test_get_line() -> Result<(), Box<dyn std::error::Error>> {
    println!("正在运行 test_get_line 测试...");
    let cache = AsyncLineCache::new();

    // 创建一个真实文件
    let filename = "test_file1.txt";
    let mut file = fs::File::create(filename)?;
    writeln!(file, "Line 1\nLine 2\nLine 3\nLine 4\nLine 5")?;

    // 设置超时时间
    let timeout_duration = Duration::from_secs(5); // 5秒超时
    println!("正在从文件 {} 中获取第 3 行...", filename);

    // 使用 timeout 包裹 get_line 方法
    match timeout(timeout_duration, cache.get_line(filename, 3)).await {
        Ok(result) => {
            match result {
                Ok(line) => {
                    println!("获取的行内容: {:?}", line);
                    assert_eq!(line.unwrap(), "Line 3");
                }
                Err(e) => {
                    println!("获取行内容时发生错误: {:?}", e);
                    return Err(e.into()); // 将错误转换为 Box<dyn std::error::Error>
                }
            }
        }
        Err(_) => {
            println!("从文件 {} 中获取行内容超时", filename);
            return Err("获取行内容时发生超时".into()); // 超时错误
        }
    }

    // 删除测试文件
    println!("正在删除测试文件: {}", filename);
    std::fs::remove_file(filename)?;

    println!("测试通过");
    Ok(())
}

#[tokio::test]
async fn test_get_lines() -> Result<(), Box<dyn std::error::Error>> {
    println!("开始运行 test_get_lines 测试..."); // 添加日志输出

    let cache = AsyncLineCache::new();

    // 创建一个真实文件并写入内容
    let filename = "test_file.txt";
    let mut file = std::fs::File::create(filename)?; // 创建文件
    writeln!(file, "Line 1\nLine 2\nLine 3")?; // 写入三行内容
    println!("已创建文件 {} 并写入内容", filename); // 添加日志输出

    // 调用 get_lines 方法获取所有行
    let lines = cache.get_lines(filename).await?;
    println!("获取的文件内容: {:?}", lines); // 添加日志输出

    // 验证返回的行数据是否正确
    assert!(lines.is_some(), "未获取到文件内容"); // 确保返回 Some
    assert_eq!(lines.unwrap(), vec!["Line 1", "Line 2", "Line 3"]); // 验证内容

    // 删除测试文件
    println!("正在删除测试文件: {}", filename); // 添加日志输出
    std::fs::remove_file(filename)?; // 删除文件

    println!("test_get_lines 测试通过"); // 添加日志输出
    Ok(())
}

#[tokio::test]
async fn test_random_line() -> Result<(), Box<dyn std::error::Error>> {
    println!("开始运行 test_random_line 测试..."); // 添加日志输出
    let cache = AsyncLineCache::new();

    // 创建一个临时文件
    let mut file = NamedTempFile::new()?;
    let filename = file.path().to_str().unwrap().to_string();
    println!("已创建临时文件: {}", filename); // 添加日志输出

    // 向文件中写入内容
    writeln!(file, "Line 1\nLine 2\nLine 3\nLine 4\nLine 5")?;
    println!("已向文件写入内容"); // 添加日志输出

    // 从缓存中随机获取一行
    let line = cache.random_line(&filename).await?;
    println!("获取的随机行内容: {:?}", line); // 添加日志输出
    assert!(line.is_some(), "未获取到随机行内容");

    println!("test_random_line 测试通过"); // 添加日志输出
    Ok(())
}

#[tokio::test]
async fn test_clear_cache() {
    // 定义文件名
    let filename = "test_file3.txt";

    // 创建文件并写入内容
    let mut file = fs::File::create(filename).expect("创建文件失败");
    writeln!(file, "Line 1").expect("写入文件失败");
    writeln!(file, "Line 2").expect("写入文件失败");
    writeln!(file, "Line 3").expect("写入文件失败");
    file.flush().expect("刷新文件失败");

    let cache = AsyncLineCache::new();

    // 加载文件并缓存
    let lineno = 1;
    println!("正在加载文件并缓存...");
    let result = cache.get_line(filename, lineno).await;
    assert!(result.is_ok(), "文件加载失败");

    // 检查缓存是否被正确加载
    let key = format!("{}:{}", filename, lineno);
    assert!(cache.cache.get(&key).is_some(), "缓存中应包含该行内容");

    // 清空缓存
    println!("正在清空缓存...");
    cache.clear_cache().await;

    // 检查缓存是否被清空
    println!("检查缓存是否被清空...");
    assert!(cache.cache.get(&key).is_none(), "行内容缓存应已被清空");
    assert!(cache.file_times.get(filename).is_none(), "文件修改时间缓存应已被清空");
    assert!(cache.file_lines.get(filename).is_none(), "文件行缓存应已被清空");

    println!("缓存清空测试通过");

    // 测试完成后，删除文件
    fs::remove_file(filename).expect("删除文件失败");
    println!("文件已删除");
}