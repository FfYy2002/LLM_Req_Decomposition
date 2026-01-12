# -*- coding: utf-8 -*-
"""
需求对清洗脚本（基于 Qwen API）

功能说明：
- 读取 output_batches/ 目录下所有 extracted_requirements_*.xlsx 文件
- 对每一条 (高级需求, 细节需求) 调用 Qwen-Max 判断是否符合规范
- 删除无效条目，保留有效需求对
- 输出清洗后结果到 clean_output/ 目录

作者：minefan
日期：2025-12-05
"""

import os
import pandas as pd
from openai import OpenAI
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import re

# =============================================================================
# 全局配置
# =============================================================================

# 输入目录：存放原始提取结果（由 extract_requirements_api.py 生成）
INPUT_DIR = "output_batches"

# 输出目录：存放清洗后的结果
OUTPUT_DIR = "clean_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# DashScope API Key
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

# 并发控制（Qwen-Max RPM=600 → QPS≈10）
MAX_CONCURRENT = 8
semaphore = threading.Semaphore(MAX_CONCURRENT)


# =============================================================================
# LLM 判断函数
# =============================================================================

def is_valid_requirement_pair(high_level: str, low_level: str, max_retries: int = 2) -> bool:
    """
    调用 Qwen-Max 判断一对需求是否符合规范。

    返回 True 表示有效，False 表示应删除。
    """
    prompt = (
        "You are a strict requirements quality checker.\n\n"

        "Given a high-level function and a sub-function, determine if the pair meets ALL of the following criteria:\n"
        "1. The high-level item is a composite functional capability (e.g., 'User Management', 'Report Generation').\n"
        "2. The sub-item is an independent, actionable function (e.g., 'Add user', 'Export PDF report'), NOT:\n"
        "   - A data field (e.g., 'email', 'status')\n"
        "   - A state or status (e.g., 'active', 'pending')\n"
        "   - A constraint or format (e.g., 'max 255 chars', 'CSV only')\n"
        "   - An enumeration (e.g., 'Red, Green, Blue')\n"
        "   - A UI step without standalone meaning (e.g., 'click Save', 'enter password')\n"
        "3. The relationship is EXPLICITLY hierarchical in the original document (not inferred).\n\n"

        "Respond ONLY with 'YES' or 'NO'. Do not explain.\n\n"

        f"High-Level Function: {high_level}\n"
        f"Sub-Function: {low_level}"
    )

    for attempt in range(max_retries + 1):
        try:
            with semaphore:
                response = CLIENT.chat.completions.create(
                    model="qwen-max",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    stream=False
                )
                answer = response.choices[0].message.content.strip().upper()
                return answer == "YES"
        except Exception as e:
            if attempt < max_retries:
                time.sleep(1)  # 短暂等待后重试
                continue
            else:
                print(f"[API ERROR] 无法判断条目 | 高级: {high_level} | 细节: {low_level} | 原因: {e}")
                return False  # 默认视为无效，安全起见
    return False


# =============================================================================
# 主流程
# =============================================================================

def process_excel_file(filepath: str) -> pd.DataFrame:
    """
    读取一个 Excel 文件，逐行验证，返回清洗后的 DataFrame。
    """
    df = pd.read_excel(filepath)
    print(f"\n[INFO] 正在清洗: {os.path.basename(filepath)} | 共 {len(df)} 条")

    valid_rows = []
    total = len(df)

    # 使用线程池并发验证
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT * 2) as executor:
        futures = []
        for _, row in df.iterrows():
            high = str(row.get("高级需求", "")).strip()
            low = str(row.get("细节需求", "")).strip()
            source = str(row.get("来源文件", ""))
            futures.append(executor.submit(is_valid_requirement_pair, high, low))

        for i, future in enumerate(as_completed(futures)):
            is_valid = future.result()
            if is_valid:
                # 重新获取对应行（注意：as_completed 顺序乱，需按原顺序匹配）
                # 改为：提前绑定索引
                pass  # 我们换一种更安全的方式

    # 更安全：逐行提交并保留索引
    valid_indices = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT * 2) as executor:
        future_to_index = {}
        for idx, row in df.iterrows():
            high = str(row["高级需求"]).strip()
            low = str(row["细节需求"]).strip()
            if not high or not low:
                continue
            future = executor.submit(is_valid_requirement_pair, high, low)
            future_to_index[future] = idx

        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            if future.result():
                valid_indices.append(idx)

    cleaned_df = df.loc[valid_indices].reset_index(drop=True)
    print(f"  → 保留 {len(cleaned_df)} / {total} 条")
    return cleaned_df


def main():
    # 扫描所有 Excel 文件
    excel_files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.startswith("extracted_requirements_") and f.endswith(".xlsx")
    ]

    if not excel_files:
        print(f"[FATAL] 在 {INPUT_DIR} 中未找到任何提取结果文件。")
        return

    print(f"[INFO] 发现 {len(excel_files)} 个待清洗文件")

    all_cleaned_data = []

    for file_path in excel_files:
        cleaned_df = process_excel_file(file_path)
        all_cleaned_data.append(cleaned_df)

        # 同时保存单个清洗文件（可选）
        base_name = os.path.basename(file_path)
        clean_path = os.path.join(OUTPUT_DIR, f"clean_{base_name}")
        cleaned_df.to_excel(clean_path, index=False, engine="openpyxl")
        print(f"  ✅ 已保存清洗文件: {clean_path}")

    print(f"\n🎉 所有 {len(excel_files)} 个文件清洗完成，结果已保存至 '{OUTPUT_DIR}' 目录。")


if __name__ == "__main__":
    main()