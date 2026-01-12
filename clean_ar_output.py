# -*- coding: utf-8 -*-
"""
AR 清洗脚本（LLM 判断 + 自动删除总结尾句）

功能：
- 读取 ar_output/ar_extracted_requirements_*.xlsx
- 自动移除 AR 中的总结性尾句（如“综上所述”、“以上是...”）
- 调用 LLM 判断清洗后的内容是否合规（仅检查：编号格式、无反问、无截断），仅保留 AR_高级需求 和 AR_细节需求 都合规的行
- 输出到 clean_ar_output/

作者：minefan
日期：2025年12月10日
"""

import os
import pandas as pd
from openai import OpenAI
import time
import threading
import re

# =============================================================================
# 配置
# =============================================================================
INPUT_DIR = "ar_output"
OUTPUT_DIR = "clean_ar_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise EnvironmentError(
        "❌ 环境变量 DASHSCOPE_API_KEY 未设置。\n"
        "请在运行前执行：\n"
        "  Windows (PowerShell): $env:DASHSCOPE_API_KEY='sk-xxx'\n"
        "  Linux/macOS: export DASHSCOPE_API_KEY='sk-xxx'"
    )

CLIENT = OpenAI(
    api_key=DASHSCOPE_API_KEY,
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

MAX_CONCURRENT = 6
semaphore = threading.Semaphore(MAX_CONCURRENT)


# =============================================================================
# 辅助函数：移除总结性尾句
# =============================================================================
def remove_summary_tail(text: str) -> str:
    if not text or pd.isna(text):
        return text
    s = str(text).strip()
    lines = s.split('\n')
    cleaned_lines = []

    # 总结关键词（不含“说明：”）
    summary_patterns = [
        r"综上所述[，。:：]*",
        r"以上.*开发需求[，。]*",
        r"以上.*内容[，。]*",
        r"上述.*需求[，。]*",
        r"总的来说[，。]*",
        r"因此，.*$",
        r"故.*$",
        r"最终.*$",
        r"（完）",
        r"【结束】",
        r"输出结束",
        r"生成完毕",
        r"以上即为.*",
        r"如上所述.*"
    ]

    # 从后往前扫描，跳过总结句
    skip = True
    for line in reversed(lines):
        stripped = line.strip()
        is_summary = any(re.search(pat, stripped, re.IGNORECASE) for pat in summary_patterns)
        if skip and is_summary:
            continue
        else:
            skip = False
            cleaned_lines.append(line)

    result = '\n'.join(reversed(cleaned_lines)).strip()
    return result if result else s


# =============================================================================
# LLM 合规判断（使用优化后的严格提示词）
# =============================================================================
VALIDATION_PROMPT = """你是一位严格的需求工程质检员。请判断以下“开发需求”文本是否完全符合 AR（Acceptance Requirement）规范。

【AR 规范硬性要求】
1. 必须是以阿拉伯数字加点开头的编号列表（如 "1. ..."），至少包含 2 项。
2. 内容必须完整，不能被截断（如结尾是逗号、冒号、省略号或半句话）。
3. ❌ 绝对禁止出现以下任何情况：
   - 请求用户提供更多信息（例如：“请提供...”、“需要更详细的需求”）
   - 表示因信息不足无法生成（例如：“由于未提供...”、“无法确定...”）
   - 使用反问或疑问语气（即使没有问号）
   - 出现“不清楚”、“不确定”、“建议补充”等推诿性表述

【判断规则】
- 只要存在上述任一违规，即为“不合规”。
- 即使内容看起来“合理”或“礼貌”，只要包含请求/推诿/不确定语义，就是不合规。
- 不要同情，不要宽容，只按规则判断。

请仅回答一个词："合规" 或 "不合规"，不要解释、不要加标点、不要换行。

开发需求文本：
{ar_text}
"""


def is_ar_valid_by_llm(ar_text: str, max_retries=2) -> bool:
    if not ar_text or pd.isna(ar_text):
        return False

    # 先移除总结尾句（预处理）
    cleaned_text = remove_summary_tail(ar_text)
    if not cleaned_text.strip():
        return False

    prompt = VALIDATION_PROMPT.format(ar_text=cleaned_text.strip()[:1500])

    for attempt in range(max_retries + 1):
        try:
            with semaphore:  # 限制并发
                response = CLIENT.chat.completions.create(
                    model="qwen-plus",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    timeout=15
                )
            answer = response.choices[0].message.content.strip()
            return "合规" in answer
        except Exception as e:
            if attempt >= max_retries:
                print(f"[WARN] LLM 判断失败，视为不合规 | 原因: {e}")
                return False
            time.sleep(1)
    return False


# =============================================================================
# 处理单个文件
# =============================================================================
def clean_file(filepath):
    df = pd.read_excel(filepath)
    total = len(df)
    print(f"  → 正在校验 {total} 行...")

    valid_rows = []
    for idx, row in df.iterrows():
        ar_high = row["AR_高级需求"]
        ar_low = row["AR_细节需求"]

        valid_high = is_ar_valid_by_llm(ar_high)
        valid_low = is_ar_valid_by_llm(ar_low)

        if valid_high and valid_low:
            # 保存的是 已清洗总结尾句 的版本
            valid_rows.append({
                "原始_高级需求": row["原始_高级需求"],
                "AR_高级需求": remove_summary_tail(ar_high),
                "原始_细节需求": row["原始_细节需求"],
                "AR_细节需求": remove_summary_tail(ar_low),
            })

        if (idx + 1) % 20 == 0:
            print(f"    已处理 {idx+1}/{total}")

    return pd.DataFrame(valid_rows)


# =============================================================================
# 主函数
# =============================================================================
def main():
    files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.startswith("ar_extracted_requirements_") and f.endswith(".xlsx")
    ]

    if not files:
        print(f"[!] 在 {INPUT_DIR} 中未找到 AR 文件")
        return

    print(f"[INFO] 共 {len(files)} 个文件待清洗")

    for fp in files:
        try:
            base = os.path.basename(fp)
            cleaned_df = clean_file(fp)
            out_path = os.path.join(OUTPUT_DIR, "clean_" + base)
            cleaned_df.to_excel(out_path, index=False, engine="openpyxl")
            original_count = len(pd.read_excel(fp))
            print(f"✅ {base} → 保留 {len(cleaned_df)} / {original_count} 行")
        except Exception as e:
            print(f"[SKIP] {fp} | 错误: {e}")

    print(f"\n🎉 清洗完成，结果保存至 '{OUTPUT_DIR}'")


if __name__ == "__main__":
    main()