# -*- coding: utf-8 -*-
"""
评估 LLM 拆分结果的覆盖质量

- 输入：decomposed_output/decomposed_requirements_*.xlsx
- 输出：evaluation_output/evaluated_decomposed_requirements_*.xlsx
- 特点：每行仅调用 1 次 LLM，判断整体覆盖情况

作者：minefan
日期：2025年12月10日
"""

import os
import pandas as pd
from openai import OpenAI
import threading
import time
import json
import re

# =============================================================================
# 配置
# =============================================================================
INPUT_DIR = "decomposed_output"
OUTPUT_DIR = "evaluation_output"

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise EnvironmentError(
        "❌ 请设置环境变量 DASHSCOPE_API_KEY\n"
        "例如：export DASHSCOPE_API_KEY='sk-xxx'"
    )

CLIENT = OpenAI(
    api_key=DASHSCOPE_API_KEY,
    base_url="https://dashscope.aliyuncs.com/v1",  # ✅ 正确 endpoint
)
MAX_CONCURRENT = 6
semaphore = threading.Semaphore(MAX_CONCURRENT)


# =============================================================================
# 工具函数：提取编号需求点
# =============================================================================
def extract_numbered_points(text):
    """从标准化文本中提取所有 '1. ...' 格式的需求点"""
    if not isinstance(text, str) or not text.strip():
        return []
    points = re.findall(
        r"^\s*\d+\.\s*(.+?)(?=\n\s*\d+\.|\n\s*边界或异常场景处理|\n\s*说明：|$)",
        text,
        re.MULTILINE | re.DOTALL
    )
    cleaned = [p.strip().rstrip("。.") for p in points if p.strip()]
    return cleaned


# =============================================================================
# 全局评估 Prompt（核心）
# =============================================================================
GLOBAL_EVAL_PROMPT_TEMPLATE = """你是一位严谨的需求工程专家，请严格判断 LLM 生成的需求点是否完整覆盖人工标准答案中的每一条。

【人工标准答案】（编号从1开始）：
{gt_list_str}

【LLM 生成结果】：
{pred_list_str}

【判断规则】
1. 仅当 LLM 点完整表达了人工点的功能、触发条件、系统行为（允许表述不同），才算覆盖。
2. 如果 LLM 点只覆盖部分内容（如只提“校验账号”但没提“密码”），不算覆盖。
3. 一个 LLM 点最多只能用于覆盖一个人工点（不可重复使用）。

【输出要求】
- 返回一个 JSON 对象：{{"covered_indices": [整数列表]}}
- 列表中的整数是被覆盖的人工点编号（从1开始）
- 不要任何解释、注释、markdown 或额外内容

示例输出：
{{"covered_indices": [1, 3]}}
"""


def evaluate_coverage(gt_points, pred_points):
    """
    调用 LLM 一次，判断哪些人工点被覆盖。
    返回：covered_indices (list of int, 从1开始)
    """
    if not gt_points:
        return []

    # 限制长度防超限（qwen-max 上下文足够，但保险起见）
    MAX_POINTS = 15
    gt_display = gt_points[:MAX_POINTS]
    pred_display = pred_points[:MAX_POINTS]

    gt_list_str = "\n".join(f"{i+1}. {p}" for i, p in enumerate(gt_display))
    pred_list_str = "\n".join(f"{i+1}. {p}" for i, p in enumerate(pred_display))

    prompt = GLOBAL_EVAL_PROMPT_TEMPLATE.format(
        gt_list_str=gt_list_str,
        pred_list_str=pred_list_str
    )

    for attempt in range(3):
        try:
            with semaphore:
                response = CLIENT.chat.completions.create(
                    model="qwen-max",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    timeout=30
                )
            raw_output = response.choices[0].message.content.strip()

            # 提取 JSON
            json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
            if json_build := json_match:
                try:
                    result = json.loads(json_build.group())
                    indices = result.get("covered_indices", [])
                    if isinstance(indices, list):
                        # 过滤有效编号（1 ~ len(gt_points)）
                        valid_indices = [
                            idx for idx in indices
                            if isinstance(idx, int) and 1 <= idx <= len(gt_points)
                        ]
                        return valid_indices
                except json.JSONDecodeError:
                    pass

            # Fallback: 尝试从文本中提取数字列表
            numbers = re.findall(r"\b\d+\b", raw_output)
            indices = [int(n) for n in numbers if 1 <= int(n) <= len(gt_points)]
            return sorted(set(indices))

        except Exception as e:
            if attempt == 2:
                print(f"    ⚠️ LLM 调用失败（已重试3次）: {str(e)[:100]}")
                return []
            time.sleep(2)

    return []


def process_file(filepath: str):
    filename = os.path.basename(filepath)
    print(f"\n[INFO] 正在评估: {filename}")

    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f"  ✘ 读取失败: {e}")
        return

    required_cols = ["AR_细节需求", "LLM_AR_细节需求"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"  ⚠ 跳过：缺少必要列 {missing}")
        return

    tps, fns, fps, recalls, precs, f1s = [], [], [], [], [], []
    gt_counts = []
    pred_counts = []

    total_rows = len(df)
    for idx, row in df.iterrows():
        gt_text = row.get("AR_细节需求", "")
        pred_text = row.get("LLM_AR_细节需求", "")

        gt_points = extract_numbered_points(gt_text)
        pred_points = extract_numbered_points(pred_text)

        gt_count = len(gt_points)
        pred_count = len(pred_points)

        covered_indices = evaluate_coverage(gt_points, pred_points)
        tp = len(covered_indices)
        fn = gt_count - tp
        fp = pred_count - tp  # 近似：假设每个覆盖消耗一个 pred 点

        recall = tp / gt_count if gt_count > 0 else 1.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        tps.append(tp)
        fns.append(fn)
        fps.append(fp)
        recalls.append(round(recall, 3))
        precs.append(round(precision, 3))
        f1s.append(round(f1, 3))
        gt_counts.append(gt_count)
        pred_counts.append(pred_count)

        if (idx + 1) % 5 == 0:
            print(f"    已完成 {idx + 1}/{total_rows} 行")

    # 写入新列
    df["人工需求点数量"] = gt_counts
    df["LLM需求点数量"] = pred_counts
    df["TP"] = tps
    df["FN"] = fns
    df["FP"] = fps
    df["拆分召回率"] = recalls
    df["拆分精确率"] = precs
    df["F1"] = f1s

    # 保存
    output_filename = filename.replace("decomposed_requirements_", "evaluated_decomposed_requirements_", 1)
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    try:
        df.to_excel(output_path, index=False, engine="openpyxl")
        print(f"  ✓ 评估完成！结果已保存至: {output_path}")
    except Exception as e:
        print(f"  ✘ 保存失败: {e}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_files = [
        f for f in os.listdir(INPUT_DIR)
        if f.endswith(".xlsx")
           and not f.startswith("~$")
           and f.startswith("decomposed_requirements_")
    ]

    if not input_files:
        raise FileNotFoundError(
            f"在目录 '{INPUT_DIR}' 中未找到符合 'decomposed_requirements_*.xlsx' 命名规范的文件"
        )

    print(f"[INFO] 共发现 {len(input_files)} 个待评估文件")

    for filename in input_files:
        filepath = os.path.join(INPUT_DIR, filename)
        process_file(filepath)

    # 全局汇总
    all_dfs = []
    for f in os.listdir(OUTPUT_DIR):
        if f.startswith("evaluated_decomposed_requirements_") and f.endswith(".xlsx"):
            df = pd.read_excel(os.path.join(OUTPUT_DIR, f))
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        total_tp = combined["TP"].sum()
        total_fn = combined["FN"].sum()
        total_fp = combined["FP"].sum()
        macro_recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
        macro_precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
        macro_f1 = 2 * macro_precision * macro_recall / (macro_precision + macro_recall) if (macro_precision + macro_recall) > 0 else 0

        print(f"\n📊 全局评估结果:")
        print(f"   总 TP: {total_tp}, FN: {total_fn}, FP: {total_fp}")
        print(f"   宏观召回率: {macro_recall:.3f}")
        print(f"   宏观精确率: {macro_precision:.3f}")
        print(f"   宏观 F1: {macro_f1:.3f}")

    print(f"\n🎉 所有文件评估已完成！结果保存在目录: '{OUTPUT_DIR}/'")


if __name__ == "__main__":
    main()