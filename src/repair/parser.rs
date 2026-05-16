//! 递归下降 JSON 修复解析器
//!
//! 翻译自 Python json_repair 项目，处理 LLM 输出的常见 JSON 问题：
//! - 混合引号（单引号/双引号/智能引号）
//! - 未引用的键和字符串
//! - 多余逗号 / 缺少逗号
//! - 截断的 JSON（缺少闭合括号）
//! - 注释（# // /* */)
//! - 布尔/null 字面量（true/false/True/False/None）
//! - 转义序列修复
//! - Markdown 代码块包裹

use serde_json::Value;
use std::fmt;

/// 解析上下文
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseContext {
    ObjectKey,
    ObjectValue,
    Array,
}

/// JSON 修复错误
#[derive(Debug)]
pub struct RepairError {
    pub message: String,
}

impl fmt::Display for RepairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JSON repair error: {}", self.message)
    }
}

impl std::error::Error for RepairError {}

/// 字符串分隔符集合
const STRING_DELIMITERS: &[char] = &['"', '\'', '\u{201c}', '\u{201d}'];

/// 数字字符集合
const NUMBER_CHARS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '.', 'e', 'E', '/', ',', '_',
];

/// 递归下降 JSON 修复解析器
pub(crate) struct JsonParser<'a> {
    input: &'a str,
    pos: usize,
    context: Vec<ParseContext>,
    stream_stable: bool,
}

impl<'a> JsonParser<'a> {
    pub fn new(input: &'a str, stream_stable: bool) -> Self {
        Self {
            input,
            pos: 0,
            context: Vec::new(),
            stream_stable,
        }
    }

    /// 获取当前字符
    fn current_char(&self) -> Option<char> {
        self.peek_char(0)
    }

    /// 获取偏移位置的字符
    fn peek_char(&self, offset: usize) -> Option<char> {
        let chars: Vec<char> = self.input[self.pos..].chars().take(offset + 1).collect();
        chars.get(offset).copied()
    }

    /// 前进一步（一个字符）
    fn advance(&mut self) {
        if let Some(c) = self.current_char() {
            self.pos += c.len_utf8();
        }
    }

    /// 获取当前位置到末尾的子串
    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    /// 是否到达末尾
    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    /// 跳过空白字符
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.current_char() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// 向前扫描空白字符（不移动 pos），返回偏移量
    fn scroll_whitespace(&self, start_offset: usize) -> usize {
        let mut offset = start_offset;
        let mut count = 0;
        for c in self.input[self.pos..].chars() {
            if count < offset {
                count += 1;
                continue;
            }
            if c.is_whitespace() {
                offset += 1;
            } else {
                break;
            }
        }
        offset
    }

    /// 向前扫描到目标字符（跳过转义的），返回偏移量
    fn skip_to_character(&self, targets: &[char], start_offset: usize) -> usize {
        let mut offset = start_offset;
        let mut backslashes = 0u32;
        let mut iter = self.input[self.pos..].chars();
        // 跳过 start_offset 个字符
        for _ in 0..start_offset {
            iter.next();
        }
        for c in iter {
            if c == '\\' {
                backslashes += 1;
                offset += 1;
                continue;
            }
            if targets.contains(&c) && backslashes.is_multiple_of(2) {
                return offset;
            }
            backslashes = 0;
            offset += 1;
        }
        offset
    }

    fn push_context(&mut self, ctx: ParseContext) {
        self.context.push(ctx);
    }

    fn pop_context(&mut self) {
        self.context.pop();
    }

    fn current_context(&self) -> Option<ParseContext> {
        self.context.last().copied()
    }

    fn has_context(&self, ctx: ParseContext) -> bool {
        self.context.contains(&ctx)
    }

    fn clear_context(&mut self) {
        self.context.clear();
    }

    /// 主入口：解析并返回修复后的 Value
    pub fn parse(mut self) -> Result<Value, RepairError> {
        let result = self.parse_json()?;
        // 处理多个顶层 JSON 值
        if !self.is_eof() {
            self.skip_whitespace();
            if !self.is_eof() {
                // 尝试收集多个值
                let mut values = vec![result];
                while !self.is_eof() {
                    self.clear_context();
                    self.skip_whitespace();
                    if self.is_eof() {
                        break;
                    }
                    match self.parse_json() {
                        Ok(v) => {
                            // 跳过顶层空值（由跳过非 JSON 字符产生）
                            if v == Value::String(String::new()) {
                                continue;
                            }
                            values.push(v);
                        }
                        Err(_) => {
                            self.advance();
                        }
                    }
                }
                if values.len() == 1 {
                    return Ok(values.into_iter().next().unwrap());
                }
                return Ok(Value::Array(values));
            }
        }
        Ok(result)
    }

    /// 解析下一个 JSON 值
    fn parse_json(&mut self) -> Result<Value, RepairError> {
        loop {
            let char = match self.current_char() {
                Some(c) => c,
                None => return Ok(Value::String(String::new())),
            };

            match char {
                '{' => {
                    self.advance();
                    return self.parse_object();
                }
                '[' => {
                    self.advance();
                    return self.parse_array("]");
                }
                '"' | '\'' | '\u{201c}' | '\u{201d}' => {
                    if self.context.is_empty() {
                        // 顶层字符串，尝试解析为值
                        let s = self.parse_string()?;
                        return Ok(Value::String(s));
                    }
                    let s = self.parse_string()?;
                    return Ok(Value::String(s));
                }
                '#' | '/' => {
                    self.parse_comment()?;
                    continue;
                }
                _ => {
                    // 在有上下文时处理未引用的值
                    if !self.context.is_empty() {
                        if char.is_alphabetic() {
                            // 可能是布尔值/null
                            if let Some(v) = self.try_parse_boolean_or_null() {
                                return Ok(v);
                            }
                            // 未引用的字符串
                            let s = self.parse_unquoted_string()?;
                            return Ok(Value::String(s));
                        }
                        if char.is_ascii_digit() || char == '-' || char == '.' {
                            return self.parse_number();
                        }
                    }
                    // 顶层无上下文，跳过非 JSON 字符
                    self.advance();
                }
            }
        }
    }

    /// 解析对象
    fn parse_object(&mut self) -> Result<Value, RepairError> {
        self.push_context(ParseContext::ObjectKey);
        let mut obj = serde_json::Map::new();

        loop {
            self.skip_whitespace();
            let c = match self.current_char() {
                Some(c) => c,
                None => break,
            };

            if c == '}' {
                self.advance();
                break;
            }

            // 跳过注释
            if c == '#' || c == '/' {
                self.parse_comment()?;
                continue;
            }

            // 处理 key 前的冒号（错误修复）
            if c == ':' {
                self.advance();
                continue;
            }

            // 解析 key
            self.context.pop();
            self.push_context(ParseContext::ObjectKey);
            let key = match self.parse_object_key() {
                Ok(k) => k,
                Err(_) => break,
            };

            // key 为空且没有更多内容 → 停止
            if key.is_empty() {
                self.skip_whitespace();
                if self.current_char() == Some('}') {
                    self.advance();
                    break;
                }
                // 遇到不可识别字符，跳过
                if self.current_char().is_some() {
                    self.advance();
                    continue;
                }
                break;
            }

            self.skip_whitespace();

            // 检查是否到达对象末尾
            if matches!(self.current_char(), Some('}') | None) {
                if !key.is_empty() {
                    obj.insert(key, Value::String(String::new()));
                }
                if self.current_char() == Some('}') {
                    self.advance();
                }
                break;
            }

            // 期望冒号
            match self.current_char() {
                Some(':') => {
                    self.advance();
                }
                Some(_) => {
                    // 缺少冒号，尝试继续
                }
                None => break,
            }

            // 解析 value
            self.context.pop();
            self.push_context(ParseContext::ObjectValue);
            let value = self.parse_json()?;

            // 处理重复键：后值覆盖前值
            obj.insert(key, value);

            self.context.pop();
            self.push_context(ParseContext::ObjectKey);

            // 跳过逗号和多余引号
            self.skip_whitespace();
            match self.current_char() {
                Some(',') | Some('\'') | Some('"') => {
                    self.advance();
                }
                Some(']') if self.has_context(ParseContext::Array) => {
                    // 在数组内的对象遇到数组结束符
                    break;
                }
                _ => {}
            }
            self.skip_whitespace();
        }

        self.pop_context();
        Ok(Value::Object(obj))
    }

    /// 解析对象键
    fn parse_object_key(&mut self) -> Result<String, RepairError> {
        self.skip_whitespace();
        match self.current_char() {
            Some('"') | Some('\'') | Some('\u{201c}') | Some('\u{201d}') => {
                self.parse_string()
            }
            Some(c) if c.is_alphanumeric() || c == '_' => {
                // 未引用的键
                self.parse_unquoted_string()
            }
            _ => Ok(String::new()),
        }
    }

    /// 解析数组
    fn parse_array(&mut self, closing: &str) -> Result<Value, RepairError> {
        self.push_context(ParseContext::Array);
        let mut arr = Vec::new();

        loop {
            self.skip_whitespace();
            let c = match self.current_char() {
                Some(c) => c,
                None => break,
            };

            if c == closing.chars().next().unwrap() {
                self.advance();
                break;
            }

            // 省略号忽略
            if c == '.' {
                let remaining = self.remaining();
                if remaining.starts_with("...") {
                    for _ in 0..3 {
                        self.advance();
                    }
                    continue;
                }
            }

            // 字符串后跟冒号 → 隐式对象
            if STRING_DELIMITERS.contains(&c) {
                let delimiter = c;
                let end_offset = self.skip_to_character(&[delimiter], 1);
                let ws_offset = self.scroll_whitespace(end_offset + 1);
                if self.peek_char(ws_offset) == Some(':') {
                    // 这是隐式对象，回退并解析为对象
                    let val = self.parse_object()?;
                    arr.push(val);
                } else {
                    let val = self.parse_json()?;
                    arr.push(val);
                }
            } else {
                let val = self.parse_json()?;
                // 跳过空值
                if val == Value::String(String::new())
                    && !matches!(self.current_char(), Some(']') | Some(','))
                {
                    self.advance();
                } else {
                    arr.push(val);
                }
            }

            // 跳过逗号和空白
            self.skip_whitespace();
            while matches!(self.current_char(), Some(',')) {
                self.advance();
                self.skip_whitespace();
            }
        }

        self.pop_context();
        Ok(Value::Array(arr))
    }

    /// 解析字符串（处理引号修复）
    fn parse_string(&mut self) -> Result<String, RepairError> {
        let delimiter = match self.current_char() {
            Some(c) if STRING_DELIMITERS.contains(&c) => c,
            _ => return Ok(String::new()),
        };

        // 确定配对的闭合分隔符
        let closing_delim = match delimiter {
            '\u{201c}' => '\u{201d}',
            '\u{201d}' => '\u{201c}',
            c => c,
        };

        // 快速路径：简单的双引号字符串（无转义、无换行）
        if delimiter == '"' {
            if let Some(result) = self.try_fast_parse_double_quoted() {
                return Ok(result);
            }
        }

        self.advance(); // 跳过开引号

        // 检查 markdown 代码块
        if self.current_char() == Some('`') {
            if let Some(v) = self.try_parse_json_llm_block() {
                return Ok(v);
            }
        }

        // 检查双引号
        if self.current_char() == Some(closing_delim) {
            let next = self.peek_char(1);
            if next == Some(closing_delim) {
                // 双引号 → 跳过第一个
                self.advance();
            } else {
                // 空字符串或误放引号
                let ctx = self.current_context();
                match ctx {
                    Some(ParseContext::ObjectKey) if self.peek_char(1) == Some(':') => {
                        return Ok(String::new());
                    }
                    Some(ParseContext::ObjectValue) if matches!(self.peek_char(1), Some(',') | Some('}')) => {
                        return Ok(String::new());
                    }
                    Some(ParseContext::Array) if matches!(self.peek_char(1), Some(',') | Some(']')) => {
                        return Ok(String::new());
                    }
                    _ => {}
                }
            }
        }

        // 扫描字符串体
        let mut result = String::new();
        loop {
            let c = match self.current_char() {
                Some(c) => c,
                None => {
                    if self.stream_stable && result.ends_with('\\') {
                        result.pop();
                    }
                    break;
                }
            };

            // 到达闭合分隔符
            if c == closing_delim && (!result.ends_with('\\') || result.ends_with("\\\\")) {
                self.advance();
                break;
            }

            // 处理转义序列
            if c == '\\' {
                self.advance();
                match self.current_char() {
                    Some('"') => {
                        result.push('"');
                        self.advance();
                    }
                    Some('n') => {
                        result.push('\n');
                        self.advance();
                    }
                    Some('r') => {
                        result.push('\r');
                        self.advance();
                    }
                    Some('t') => {
                        result.push('\t');
                        self.advance();
                    }
                    Some('b') => {
                        result.push('\u{08}');
                        self.advance();
                    }
                    Some('f') => {
                        result.push('\u{0c}');
                        self.advance();
                    }
                    Some('\\') => {
                        result.push('\\');
                        self.advance();
                    }
                    Some('/') => {
                        result.push('/');
                        self.advance();
                    }
                    Some('u') => {
                        self.advance();
                        // 读取 4 个十六进制字符
                        let hex: String = self.remaining().chars().take(4).collect();
                        if hex.len() == 4 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                            if let Ok(code) = u32::from_str_radix(&hex, 16) {
                                if let Some(ch) = char::from_u32(code) {
                                    result.push(ch);
                                }
                                for _ in 0..4 {
                                    self.advance();
                                }
                            }
                        }
                    }
                    Some(nc) if nc == closing_delim => {
                        // 不该转义的分隔符
                        result.push(nc);
                        self.advance();
                    }
                    Some(nc) => {
                        // 未知的转义序列，保留反斜杠和字符
                        result.push('\\');
                        result.push(nc);
                        self.advance();
                    }
                    None => {
                        result.push('\\');
                        break;
                    }
                }
                continue;
            }

            // 未引用键在遇到冒号或空白时停止
            if matches!(self.current_context(), Some(ParseContext::ObjectKey))
                && (c == ':' || (c.is_whitespace() && !result.is_empty()))
            {
                break;
            }

            // 数组上下文中未引用字符串在遇到 ] 或 , 时停止
            if matches!(self.current_context(), Some(ParseContext::Array)) && (c == ']' || c == ',')
            {
                break;
            }

            result.push(c);
            self.advance();
        }

        Ok(result.trim_end().to_string())
    }

    /// 快速路径：解析简单双引号字符串
    fn try_fast_parse_double_quoted(&mut self) -> Option<String> {
        if self.current_char() != Some('"') {
            return None;
        }
        let start = self.pos + 1; // 跳过开引号
        let rest = &self.input[start..];
        let end = rest.find('"')?;
        let value = &rest[..end];
        // 如果包含转义或换行，不能走快速路径
        if value.contains('\\') || value.contains('\n') || value.contains('\r') {
            return None;
        }

        let next_pos = start + end + 1;
        // 验证下一个字符是否合理
        let after = self.input[next_pos..].chars().next();
        match self.current_context() {
            Some(ParseContext::ObjectKey) => {
                if after != Some(':') {
                    return None;
                }
            }
            Some(ParseContext::ObjectValue) => {
                if !matches!(after, Some(',') | Some('}') | None) {
                    return None;
                }
            }
            Some(ParseContext::Array) => {
                if !matches!(after, Some(',') | Some(']') | None) {
                    return None;
                }
            }
            None => {
                if after.is_some() {
                    return None;
                }
            }
        }

        self.pos = next_pos;
        Some(value.to_string())
    }

    /// 尝试解析 markdown json 代码块
    fn try_parse_json_llm_block(&mut self) -> Option<String> {
        let remaining = self.remaining();
        if let Some(after_fence) = remaining.strip_prefix("```json") {
            let mut content_start = 0;
            let mut content_end = None;

            // 跳过代码块开头的空白
            for (i, c) in after_fence.char_indices() {
                if c == '\n' || c == '\r' {
                    content_start = i + c.len_utf8();
                } else {
                    break;
                }
            }

            let inner = &after_fence[content_start..];
            let mut idx = 0;
            for c in inner.chars() {
                if inner[idx..].starts_with("```") {
                    content_end = Some(idx);
                    break;
                }
                idx += c.len_utf8();
            }

            if let Some(end) = content_end {
                let content = inner[..end].trim();
                // 尝试解析代码块内的 JSON
                if let Ok(v) = serde_json::from_str::<Value>(content) {
                    self.pos += 7 + content_start + end + 3;
                    return Some(v.to_string());
                }
                // 如果内容是有效的，直接返回
                self.pos += 7 + content_start + end + 3;
                return Some(content.to_string());
            }
        }
        None
    }

    /// 解析未引用的字符串（裸词）
    fn parse_unquoted_string(&mut self) -> Result<String, RepairError> {
        let mut result = String::new();
        while let Some(c) = self.current_char() {
            if c.is_whitespace() || c == ':' || c == ',' || c == '}' || c == ']' || c == '{'
                || c == '['
            {
                break;
            }
            result.push(c);
            self.advance();
        }
        Ok(result)
    }

    /// 尝试解析布尔值或 null（大小写不敏感）
    fn try_parse_boolean_or_null(&mut self) -> Option<Value> {
        let c = self.current_char()?.to_ascii_lowercase();

        // 支持多种形式：true/True, false/False, null/None
        let candidates: &[(&str, Value)] = match c {
            't' => &[("true", Value::Bool(true))],
            'f' => &[("false", Value::Bool(false))],
            'n' => &[("null", Value::Null), ("none", Value::Null)],
            'y' => &[("yes", Value::Bool(true))],
            _ => return None,
        };

        let start_pos = self.pos;

        for (expected, value) in candidates {
            let mut matched = 0;
            self.pos = start_pos;

            for expected_char in expected.chars() {
                match self.current_char() {
                    Some(ch) if ch.to_ascii_lowercase() == expected_char => {
                        matched += 1;
                        self.advance();
                    }
                    _ => break,
                }
            }

            if matched == expected.len() {
                return Some(value.clone());
            }
        }

        self.pos = start_pos;
        None
    }

    /// 解析数字
    fn parse_number(&mut self) -> Result<Value, RepairError> {
        let mut num_str = String::new();
        while let Some(c) = self.current_char() {
            if !NUMBER_CHARS.contains(&c) {
                break;
            }
            if matches!(self.current_context(), Some(ParseContext::Array)) && c == ',' {
                break;
            }
            if c != '_' {
                num_str.push(c);
            }
            self.advance();
        }

        // 数字后面跟着字母 → 可能是字符串
        if let Some(c) = self.current_char() {
            if c.is_alphabetic() {
                // 回退，按字符串解析
                self.pos -= num_str.len();
                let s = self.parse_unquoted_string()?;
                return Ok(Value::String(s));
            }
        }

        // 修整末尾无效字符
        while num_str.ends_with(['-', 'e', 'E', '/', ',', '.'])
        {
            num_str.pop();
        }

        if num_str.is_empty() {
            return Ok(Value::String(String::new()));
        }

        // 逗号分隔的数字 → 保留为字符串
        if num_str.contains(',') {
            return Ok(Value::String(num_str));
        }

        // 浮点数
        if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
            if let Ok(f) = num_str.parse::<f64>() {
                return Ok(Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| {
                    serde_json::Number::from(0)
                })));
            }
        }

        // 整数
        if let Ok(i) = num_str.parse::<i64>() {
            return Ok(Value::Number(serde_json::Number::from(i)));
        }

        Ok(Value::String(num_str))
    }

    /// 解析并跳过注释
    fn parse_comment(&mut self) -> Result<(), RepairError> {
        loop {
            match self.current_char() {
                Some('#') => {
                    // 行注释
                    while let Some(c) = self.current_char() {
                        if c == '\n' || c == '\r' {
                            break;
                        }
                        self.advance();
                    }
                }
                Some('/') => {
                    match self.peek_char(1) {
                        Some('/') => {
                            // 行注释
                            self.advance();
                            self.advance();
                            while let Some(c) = self.current_char() {
                                if c == '\n' || c == '\r' {
                                    break;
                                }
                                self.advance();
                            }
                        }
                        Some('*') => {
                            // 块注释
                            self.advance();
                            self.advance();
                            loop {
                                match self.current_char() {
                                    Some('*') if self.peek_char(1) == Some('/') => {
                                        self.advance();
                                        self.advance();
                                        break;
                                    }
                                    None => break,
                                    _ => self.advance(),
                                }
                            }
                        }
                        _ => {
                            self.advance();
                        }
                    }
                }
                _ => break,
            }

            // 顶层注释后可能还有更多注释或 JSON
            if self.context.is_empty() {
                self.skip_whitespace();
                if matches!(self.current_char(), Some('#') | Some('/')) {
                    continue;
                }
            }
            break;
        }
        Ok(())
    }
}

/// 修复 JSON 字符串，返回 serde_json::Value
pub fn repair_json(input: &str) -> Result<Value, RepairError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }

    // 快速路径：先尝试标准解析
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Ok(v);
    }

    // 使用修复解析器
    let parser = JsonParser::new(trimmed, false);
    parser.parse()
}

/// 修复 JSON 字符串，返回格式化的 JSON 字符串
pub fn repair_json_str(input: &str) -> Result<String, RepairError> {
    let value = repair_json(input)?;
    Ok(value.to_string())
}

/// 流式稳定的 JSON 修复（处理截断的 JSON）
pub fn repair_json_stream_stable(input: &str) -> Result<Value, RepairError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }

    // 快速路径
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Ok(v);
    }

    let parser = JsonParser::new(trimmed, true);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_json() {
        let result = repair_json(r#"{"key": "value"}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_single_quotes() {
        let result = repair_json(r#"{'key': 'value'}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_trailing_comma_object() {
        let result = repair_json(r#"{"key": "value",}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_trailing_comma_array() {
        let result = repair_json(r#"[1, 2, 3,]"#).unwrap();
        assert_eq!(result, json!([1, 2, 3]));
    }

    #[test]
    fn test_unquoted_keys() {
        let result = repair_json(r#"{key: "value"}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_missing_closing_brace() {
        let result = repair_json(r#"{"key": "value""#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_missing_closing_bracket() {
        let result = repair_json(r#"[1, 2, 3"#).unwrap();
        assert_eq!(result, json!([1, 2, 3]));
    }

    #[test]
    fn test_boolean_null_literals() {
        let result = repair_json(r#"[true, false, null]"#).unwrap();
        assert_eq!(result, json!([true, false, null]));
    }

    #[test]
    fn test_python_booleans() {
        let result = repair_json(r#"[True, False, None]"#).unwrap();
        assert_eq!(result, json!([true, false, null]));
    }

    #[test]
    fn test_line_comment() {
        let result = repair_json(r#"{"key": "value" // comment}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_block_comment() {
        let result = repair_json(r#"{"key": /* comment */ "value"}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_hash_comment() {
        let result = repair_json("{\n# comment\n\"key\": \"value\"}").unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_truncated_json_stream() {
        let result = repair_json_stream_stable(r#"{"key": "val"#).unwrap();
        assert_eq!(result["key"], "val");
    }

    #[test]
    fn test_smart_quotes() {
        let result = repair_json("{\u{201c}key\u{201d}: \u{201c}value\u{201d}}").unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_mixed_quotes() {
        let result = repair_json(r#"{"key": 'value'}"#).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_extra_comma_between_items() {
        let result = repair_json(r#"[1,,2]"#).unwrap();
        assert_eq!(result, json!([1, 2]));
    }

    #[test]
    fn test_number_with_trailing_dot() {
        let result = repair_json(r#"{"num": 42.}"#).unwrap();
        assert_eq!(result["num"], json!(42));
    }

    #[test]
    fn test_nested_object() {
        let result = repair_json(r#"{"outer": {"inner": "value"}}"#).unwrap();
        assert_eq!(result["outer"]["inner"], "value");
    }

    #[test]
    fn test_array_of_objects() {
        let result = repair_json(r#"[{"a": 1}, {"b": 2}]"#).unwrap();
        assert_eq!(result[0]["a"], 1);
        assert_eq!(result[1]["b"], 2);
    }

    #[test]
    fn test_unicode_escape() {
        let result = repair_json(r#"{"key": "A"}"#).unwrap();
        assert_eq!(result["key"], "A");
    }

    #[test]
    fn test_empty_input() {
        let result = repair_json("").unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_repair_json_str() {
        let result = repair_json_str(r#"{'key': 'value'}"#).unwrap();
        assert!(result.contains("\"key\""));
        assert!(result.contains("\"value\""));
    }

    #[test]
    fn test_multiple_top_level_values() {
        let result = repair_json(r#"{"a": 1}{"b": 2}"#).unwrap();
        assert!(result.is_array());
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_markdown_wrapped_json() {
        let input = "```json\n{\"key\": \"value\"}\n```";
        // 先由预处理层去 fence，再修复
        let cleaned = crate::repair::strip_markdown_fences(input);
        let result = repair_json(&cleaned).unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_deeply_nested() {
        let result = repair_json(r#"{"a": {"b": {"c": [1, 2, {"d": true}]}}}"#).unwrap();
        assert_eq!(result["a"]["b"]["c"][2]["d"], true);
    }
}
