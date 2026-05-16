/// 提示词包裹与结果提取
///
/// 通过最小干扰的提示词追加，要求 Agent 在输出中用标记包裹最终结果。
/// gege 从完整输出中按标记提取，提供稳定的下游接口。
pub const RESULT_MARKER_START: &str = "<<<GEGE_RESULT>>>";
pub const RESULT_MARKER_END: &str = "<<<GEGE_RESULT_END>>>";

/// gege 解析后的结构化结果
#[derive(Debug, Clone, Default)]
pub struct ParsedOutput {
    pub text: String,
    pub session_id: String,
    pub parsed: bool,
}

/// 在原始 prompt 末尾追加格式指令（最小干扰）
pub fn wrap_prompt(original_prompt: &str) -> String {
    format!(
        "{prompt}\n\n---\n\
         完成后，请将最终结果放在 {start} 和 {end} 之间。\
         格式如下（每行一个字段）：\n\
         result: <核心结果>\n\
         仅包裹核心结果，不要包含推理过程。",
        prompt = original_prompt,
        start = RESULT_MARKER_START,
        end = RESULT_MARKER_END,
    )
}

/// Claude stream-json 单行解析：提取可读文本
pub fn parse_claude_line(line: &str) -> crate::executors::ParsedLine {
    use crate::executors::ParsedLine;

    let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else {
        // 非 JSON，原样返回
        return ParsedLine {
            agent_text: line.to_string(),
            agent_raw: line.to_string(),
        };
    };

    let msg_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match msg_type {
        "assistant" => {
            // 从 content 数组中提取 text 类型的内容
            let mut texts = Vec::new();
            if let Some(content) = json
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_array())
            {
                for item in content {
                    if item.get("type").and_then(|v| v.as_str()) == Some("text") {
                        if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                            texts.push(text.to_string());
                        }
                    }
                }
            }
            ParsedLine {
                agent_text: texts.join("\n"),
                agent_raw: line.to_string(),
            }
        }
        "result" => {
            let text = json
                .get("result")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            ParsedLine {
                agent_text: text,
                agent_raw: line.to_string(),
            }
        }
        _ => {
            // system/hook 等消息：agent_text 为空，agent_raw 保留原文
            ParsedLine {
                agent_text: String::new(),
                agent_raw: line.to_string(),
            }
        }
    }
}

/// 从累积输出中提取 session_id（从 stream-json init 消息）
fn extract_session_id(full_output: &str) -> String {
    for line in full_output.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if json.get("type").and_then(|v| v.as_str()) == Some("system")
                && json.get("subtype").and_then(|v| v.as_str()) == Some("init")
            {
                if let Some(sid) = json.get("session_id").and_then(|v| v.as_str()) {
                    return sid.to_string();
                }
            }
        }
    }
    String::new()
}

/// 从 agent 完整输出中提取结构化结果
pub fn extract_result(full_output: &str) -> ParsedOutput {
    let session_id = extract_session_id(full_output);

    let content = match extract_between_markers(full_output) {
        Some(c) => c,
        None => {
            // 降级：尝试从 result 消息中提取
            let result_text = extract_result_message(full_output);
            return ParsedOutput {
                text: if result_text.is_empty() {
                    extract_assistant_text(full_output)
                } else {
                    result_text
                },
                session_id,
                parsed: false,
            };
        }
    };

    let mut text = String::new();
    for line in content.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("result:") {
            text = value.trim().to_string();
        } else if !line.is_empty() && !line.starts_with("sessionId:") {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(line);
        }
    }

    if text.is_empty() && !content.trim().is_empty() {
        text = content.trim().to_string();
    }

    ParsedOutput {
        text,
        session_id,
        parsed: true,
    }
}

fn extract_between_markers(output: &str) -> Option<String> {
    let start_idx = output.find(RESULT_MARKER_START)?;
    let after_start = start_idx + RESULT_MARKER_START.len();
    if let Some(end_idx) = output[after_start..].find(RESULT_MARKER_END) {
        return Some(output[after_start..after_start + end_idx].trim().to_string());
    }
    Some(output[after_start..].trim().to_string())
}

/// 从 stream-json 的 result 消息中提取文本
fn extract_result_message(output: &str) -> String {
    for line in output.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if json.get("type").and_then(|v| v.as_str()) == Some("result") {
                if let Some(result) = json.get("result").and_then(|v| v.as_str()) {
                    return result.to_string();
                }
            }
        }
    }
    String::new()
}

/// 从 stream-json 的 assistant 消息中提取文本
fn extract_assistant_text(output: &str) -> String {
    let mut texts = Vec::new();
    for line in output.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if json.get("type").and_then(|v| v.as_str()) == Some("assistant") {
                if let Some(content) = json
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_array())
                {
                    for item in content {
                        if item.get("type").and_then(|v| v.as_str()) == Some("text") {
                            if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                                texts.push(text.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    texts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_with_structured_markers() {
        let output = "<<<GEGE_RESULT>>>\nresult: 42\n<<<GEGE_RESULT_END>>>";
        let parsed = extract_result(output);
        assert!(parsed.parsed);
        assert_eq!(parsed.text, "42");
    }

    #[test]
    fn test_extract_session_id_from_init() {
        let output = r#"{"type":"system","subtype":"init","session_id":"abc-123"}
<<<GEGE_RESULT>>>
result: hello
<<<GEGE_RESULT_END>>>"#;
        let parsed = extract_result(output);
        assert!(parsed.parsed);
        assert_eq!(parsed.session_id, "abc-123");
        assert_eq!(parsed.text, "hello");
    }

    #[test]
    fn test_fallback_to_result_message() {
        let output = r#"{"type":"result","subtype":"success","result":"fallback text","duration_ms":100}"#;
        let parsed = extract_result(output);
        assert!(!parsed.parsed);
        assert_eq!(parsed.text, "fallback text");
    }

    #[test]
    fn test_fallback_to_assistant_text() {
        let output = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"assistant reply"}]}}"#;
        let parsed = extract_result(output);
        assert!(!parsed.parsed);
        assert_eq!(parsed.text, "assistant reply");
    }

    #[test]
    fn test_parse_claude_line_assistant() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#;
        let parsed = parse_claude_line(line);
        assert_eq!(parsed.agent_text, "hello");
        assert_eq!(parsed.agent_raw, line);
    }

    #[test]
    fn test_parse_claude_line_system() {
        let line = r#"{"type":"system","subtype":"init","session_id":"s1"}"#;
        let parsed = parse_claude_line(line);
        assert!(parsed.agent_text.is_empty());
        assert_eq!(parsed.agent_raw, line);
    }

    #[test]
    fn test_parse_claude_line_result() {
        let line = r#"{"type":"result","subtype":"success","result":"done"}"#;
        let parsed = parse_claude_line(line);
        assert_eq!(parsed.agent_text, "done");
    }

    #[test]
    fn test_parse_claude_line_plain_text() {
        let parsed = parse_claude_line("just text");
        assert_eq!(parsed.agent_text, "just text");
        assert_eq!(parsed.agent_raw, "just text");
    }

    #[test]
    fn test_wrap_prompt() {
        let wrapped = wrap_prompt("do something");
        assert!(wrapped.starts_with("do something"));
        assert!(wrapped.contains(RESULT_MARKER_START));
        assert!(wrapped.contains(RESULT_MARKER_END));
    }
}
