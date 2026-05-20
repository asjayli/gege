//! Agent 输出修复模块
//!
//! 翻译自 Python json_repair 项目，处理 LLM 输出的常见问题：
//! - 礼貌性自杀（"Sure! Here is..." 前缀）
//! - Markdown 代码块包裹（```json ... ```）
//! - 额外引号 / 单双引号混用
//! - JSON 截断
//! - 多余逗号
//! - 多次转义 / 不转义
//! - 未引用的键
//! - 布尔/null 字面量（True/False/None）
//! - 注释

mod parser;

pub use parser::{repair_json, repair_json_str, repair_json_stream_stable, RepairError};

use serde_json::Value;

/// 清洗后的输出结果
#[derive(Debug, Clone)]
pub struct CleanedOutput {
    /// 清洗后的文本
    pub text: String,
    /// 是否成功解析为 JSON
    pub is_json: bool,
    /// 如果是 JSON，解析后的值
    pub json_value: Option<Value>,
    /// 应用了哪些修复
    pub repairs: Vec<String>,
}

/// 礼貌性自杀前缀模式（多语言）
const POLITE_PREFIXES: &[&str] = &[
    "Sure, here is",
    "Sure! Here is",
    "Sure! Here's",
    "Sure, here's",
    "Here is",
    "Here's",
    "I'd be happy to",
    "I would be happy to",
    "Of course",
    "Certainly",
    "Absolutely",
    "I apologize",
    "I'm sorry",
    "Sorry about that",
    "Let me help",
    "Here you go",
    "There you go",
    "好的，",
    "没问题，",
    "当然，",
    "以下是",
    "当然可以",
    "当然！",
];

/// 去除 Markdown 代码块包裹
///
/// 处理 \`\`\`json ... \`\`\` 和 \`\`\` ... \`\`\` 格式
/// 支持在文本开头或中间的代码块
pub fn strip_markdown_fences(input: &str) -> String {
    let trimmed = input.trim();

    // 找到 ```json 或 ``` 的位置
    let fence_start = trimmed.find("```").unwrap_or(usize::MAX);
    if fence_start == usize::MAX {
        return input.to_string();
    }

    // 跳过 fence 头
    let after_fence_start = &trimmed[fence_start..];
    let after_header = match after_fence_start.find('\n') {
        Some(i) => &after_fence_start[i + 1..],
        None => return input.to_string(),
    };

    // 找到闭合 fence
    let content = if let Some(end) = after_header.rfind("```") {
        after_header[..end].trim()
    } else {
        // 没有闭合 fence，取 fence 头之后的内容
        after_header.trim()
    };

    // fence 前面如有礼貌性文字，只返回 fence 内容
    content.to_string()
}

/// 去除礼貌性自杀前缀
///
/// LLM 经常在 JSON 前添加礼貌用语，需要剥离
fn strip_polite_prefix(input: &str) -> (String, bool) {
    let trimmed = input.trim();
    let lower = trimmed.to_lowercase();

    for prefix in POLITE_PREFIXES {
        let prefix_lower = prefix.to_lowercase();

        // 精确前缀匹配（大小写不敏感）
        if let Some(rest) = lower.strip_prefix(&prefix_lower) {
            let prefix_byte_len = trimmed.len() - rest.len();
            let after = &trimmed[prefix_byte_len..];
            // 跳过分隔符和中间文字，找到第一个 JSON 结构字符
            let json_start = find_json_start(after);
            if let Some(start) = json_start {
                return (after[start..].to_string(), true);
            }
        }
    }

    (input.to_string(), false)
}

/// 找到字符串中第一个 JSON 结构字符的位置
fn find_json_start(input: &str) -> Option<usize> {
    for (i, c) in input.char_indices() {
        if c == '{' || c == '[' || c == '"' {
            return Some(i);
        }
    }
    None
}

/// 去除 JSON 后缀说明文字
///
/// 通过追踪括号平衡找到 JSON 真正结束的位置，截断其后的说明文字
fn strip_suffix_explanation(input: &str) -> String {
    let trimmed = input.trim();

    // 必须以 { 或 [ 开头才可能是 JSON 带后缀
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return input.to_string();
    }

    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;

    for (i, c) in trimmed.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
            if depth == 0 {
                let end = i + c.len_utf8();
                let after = trimmed[end..].trim();
                if !after.is_empty() {
                    return trimmed[..end].to_string();
                }
                return input.to_string();
            }
        }
    }

    input.to_string()
}

/// 从文本中提取 JSON 部分
///
/// 在 LLM 混合输出中找到 JSON 结构
fn extract_json_part(input: &str) -> String {
    let trimmed = input.trim();

    // 已经是纯 JSON
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return trimmed.to_string();
    }

    // 尝试找到第一个 { 或 [
    let mut obj_start = None;
    let mut arr_start = None;

    for (i, c) in trimmed.char_indices() {
        if c == '{' && obj_start.is_none() {
            obj_start = Some(i);
        }
        if c == '[' && arr_start.is_none() {
            arr_start = Some(i);
        }
    }

    let start = match (obj_start, arr_start) {
        (Some(o), Some(a)) => Some(o.min(a)),
        (Some(o), None) => Some(o),
        (None, Some(a)) => Some(a),
        (None, None) => None,
    };

    match start {
        Some(s) if s > 0 => trimmed[s..].to_string(),
        _ => input.to_string(),
    }
}

/// 清洗 Agent 输出 — 主入口
///
/// 完整的清洗流程：
/// 1. 去除礼貌性自杀前缀
/// 2. 去除 Markdown 代码块
/// 3. 提取 JSON 部分
/// 4. 去除后缀说明文字
/// 5. 尝试 JSON 修复
pub fn clean_agent_output(raw: &str) -> CleanedOutput {
    let mut text = raw.to_string();
    let mut repairs = Vec::new();

    // 1. 去除礼貌性自杀前缀
    let (without_polite, was_stripped) = strip_polite_prefix(&text);
    if was_stripped {
        repairs.push("stripped_polite_prefix".to_string());
        text = without_polite;
    }

    // 2. 去除 Markdown 代码块
    let without_fence = strip_markdown_fences(&text);
    if without_fence.len() != text.len() {
        repairs.push("stripped_markdown_fences".to_string());
        text = without_fence;
    }

    // 3. 提取 JSON 部分
    let json_part = extract_json_part(&text);
    if json_part.len() != text.len() {
        repairs.push("extracted_json_part".to_string());
        text = json_part;
    }

    // 4. 去除后缀说明文字
    let cleaned = strip_suffix_explanation(&text);
    if cleaned.len() != text.len() {
        repairs.push("stripped_suffix_explanation".to_string());
        text = cleaned;
    }

    // 5. 尝试 JSON 修复
    let trimmed = text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.starts_with('"') {
        match repair_json(trimmed) {
            Ok(v) => {
                return CleanedOutput {
                    text: v.to_string(),
                    is_json: true,
                    json_value: Some(v),
                    repairs,
                };
            }
            Err(_) => {
                repairs.push("json_repair_failed".to_string());
            }
        }
    }

    // 不是 JSON 或修复失败，返回清洗后的文本
    CleanedOutput {
        text,
        is_json: false,
        json_value: None,
        repairs,
    }
}

/// 尝试修复 Agent 输出为 JSON 字符串
///
/// 如果输出不是 JSON 格式，返回原始清洗后的文本
pub fn repair_agent_output(raw: &str) -> String {
    let cleaned = clean_agent_output(raw);
    cleaned.text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_markdown_fences() {
        let input = "```json\n{\"key\": \"value\"}\n```";
        let result = strip_markdown_fences(input);
        assert_eq!(result, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_strip_markdown_fences_no_lang() {
        let input = "```\n{\"key\": \"value\"}\n```";
        let result = strip_markdown_fences(input);
        assert_eq!(result, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_strip_markdown_fences_no_fence() {
        let input = "{\"key\": \"value\"}";
        let result = strip_markdown_fences(input);
        assert_eq!(result, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_strip_polite_prefix_english() {
        let (result, stripped) = strip_polite_prefix("Sure, here is the result: {\"a\": 1}");
        assert!(stripped);
        assert!(result.starts_with('{'));
    }

    #[test]
    fn test_strip_polite_prefix_chinese() {
        let (result, stripped) = strip_polite_prefix("好的，这是结果：{\"a\": 1}");
        assert!(stripped);
        assert!(result.starts_with('{'));
    }

    #[test]
    fn test_strip_polite_prefix_none() {
        let (result, stripped) = strip_polite_prefix("{\"a\": 1}");
        assert!(!stripped);
        assert_eq!(result, "{\"a\": 1}");
    }

    #[test]
    fn test_extract_json_part() {
        let result = extract_json_part("The result is: {\"a\": 1}");
        assert_eq!(result, "{\"a\": 1}");
    }

    #[test]
    fn test_clean_agent_output_full() {
        let input = "Sure! Here is the JSON:\n```json\n{'key': 'value'}\n```";
        let cleaned = clean_agent_output(input);
        assert!(cleaned.is_json);
        // 礼貌前缀剥离直接跳到 JSON 部分，fence 可能已被跨过
        assert!(cleaned.repairs.contains(&"stripped_polite_prefix".to_string()));
        assert_eq!(cleaned.json_value.unwrap()["key"], "value");
    }

    #[test]
    fn test_clean_agent_output_truncated() {
        let input = "好的，这是结果：\n{\"key\": \"value\"";
        let cleaned = clean_agent_output(input);
        // 应该能修复截断的 JSON
        assert!(cleaned.is_json);
    }

    #[test]
    fn test_clean_agent_output_plain_text() {
        let input = "This is just plain text, not JSON at all.";
        let cleaned = clean_agent_output(input);
        assert!(!cleaned.is_json);
        assert!(cleaned.json_value.is_none());
    }

    #[test]
    fn test_clean_mixed_quotes_and_trailing_comma() {
        let input = "{'key': 'value', 'num': 42,}";
        let cleaned = clean_agent_output(input);
        assert!(cleaned.is_json);
        assert_eq!(cleaned.json_value.unwrap()["key"], "value");
    }

    #[test]
    fn test_repair_agent_output() {
        let input = "```json\n{'key': 'value'}\n```";
        let result = repair_agent_output(input);
        assert!(result.contains("\"key\""));
        assert!(result.contains("\"value\""));
    }

    #[test]
    fn test_strip_suffix_single_line() {
        let input = r#"{"key": "value"} This is the result"#;
        let cleaned = strip_suffix_explanation(input);
        assert_eq!(cleaned, r#"{"key": "value"}"#);
    }

    #[test]
    fn test_strip_suffix_multiline() {
        let input = "{\"key\": \"value\"}\nThis is the result\nof the computation";
        let cleaned = strip_suffix_explanation(input);
        assert_eq!(cleaned, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_strip_suffix_none() {
        let input = r#"{"key": "value"}"#;
        let cleaned = strip_suffix_explanation(input);
        assert_eq!(cleaned, input);
    }

    #[test]
    fn test_clean_agent_output_with_suffix() {
        let input = "好的，这是结果：\n{\"key\": \"value\"}\n以上是输出结果";
        let cleaned = clean_agent_output(input);
        assert!(cleaned.is_json);
        assert!(cleaned.repairs.contains(&"stripped_suffix_explanation".to_string()));
    }

    #[test]
    fn test_strip_markdown_fences_unclosed() {
        let input = "```json\n{\"key\": \"value\"}\n";
        let result = strip_markdown_fences(input);
        assert_eq!(result, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_strip_markdown_fences_inline() {
        let input = "Here is the result:\n```\n{\"key\": \"value\"}\n```";
        let result = strip_markdown_fences(input);
        assert_eq!(result, "{\"key\": \"value\"}");
    }

    #[test]
    fn test_extract_json_part_array() {
        let result = extract_json_part("The array is: [1, 2, 3]");
        assert_eq!(result, "[1, 2, 3]");
    }

    #[test]
    fn test_extract_json_part_no_json() {
        let result = extract_json_part("No json here at all");
        assert_eq!(result, "No json here at all");
    }

    #[test]
    fn test_strip_suffix_explanation_array() {
        let input = "[1, 2, 3] This is extra";
        let cleaned = strip_suffix_explanation(input);
        assert_eq!(cleaned, "[1, 2, 3]");
    }

    #[test]
    fn test_strip_suffix_explanation_no_brackets() {
        let input = "plain text with no brackets";
        let cleaned = strip_suffix_explanation(input);
        assert_eq!(cleaned, input);
    }

    #[test]
    fn test_strip_polite_prefix_mixed_case() {
        let (result, stripped) = strip_polite_prefix("sUrE, hErE iS {\"a\": 1}");
        assert!(stripped);
        assert!(result.starts_with('{'));
    }

    #[test]
    fn test_clean_agent_output_only_fence() {
        let input = "```\n{\"key\": \"value\"}\n```";
        let cleaned = clean_agent_output(input);
        assert!(cleaned.is_json);
        assert_eq!(cleaned.json_value.unwrap()["key"], "value");
    }

    #[test]
    fn test_clean_agent_output_array_with_suffix() {
        let input = "Sure! Here is the result:\n[1, 2, 3]\nThat's all.";
        let cleaned = clean_agent_output(input);
        assert!(cleaned.is_json);
        assert_eq!(cleaned.json_value.unwrap(), serde_json::json!([1, 2, 3]));
    }
}
