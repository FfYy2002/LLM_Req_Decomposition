# -*- coding: utf-8 -*-
"""
使用大模型（LLM）进行拆分一致性检测

输入目录：decomposed_output/
输出目录：check_consistency_output/
输出文件名：checked_decomposed_requirements_*.xlsx

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
OUTPUT_DIR = "check_consistency_output"

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise EnvironmentError(
        "❌ 请设置环境变量 DASHSCOPE_API_KEY\n"
        "例如：export DASHSCOPE_API_KEY='sk-xxx'"
    )

CLIENT = OpenAI(
    api_key=DASHSCOPE_API_KEY,
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)
MAX_CONCURRENT = 6
semaphore = threading.Semaphore(MAX_CONCURRENT)


# =============================================================================
# 一致性检测提示词（6条规则原文逐字保留）
# =============================================================================
CONSISTENCY_PROMPT_TEMPLATE = """你是一位专业的需求工程专家，请根据以下6条一致性规则，严格评估“高级需求”与“细节需求”列表之间的拆分是否一致：

1. 每条需求应仅陈述一个能力、特性、约束或质量因素；但可以包含多个满足该需求的条件。 
2. 高级需求中的每个功能必须映射到细节需求中的唯一一个功能。 
3. 高级需求中的每个关联关系至少映射到细节需求中一个关联关系。 
4. include/extend 关系必须保留语义一致性（即不能改变执行流程含义）。 
5. 高级需求中功能之间存在结构关系，细节需求也应能表达等效逻辑。 
6. 细节需求中不能出现高级需求中未出现的功能行为。

【任务】
请综合判断细节需求是否完全满足上述所有规则。

【输出要求】
- 如果完全符合所有规则，请返回：{{"result": "通过"}}
- 如果有任何一条规则被违反，请返回：{{"result": "不通过", "reason": "具体说明违反了哪条规则，并举例指出问题所在"}}

请确保输出为合法 JSON 格式，不要包含额外解释或 markdown。

【输入】
高级需求：
{high_ar}

细节需求：
{low_ar}
"""


def call_consistency_check(high_ar: str, low_ar: str) -> str:
    """
    调用大模型进行一致性检测，返回结构化结果。
    """
    if not high_ar or not low_ar:
        return "检测失败：高级需求或细节需求为空"

    # 截断防超长（DashScope 有 token 限制）
    high_clean = str(high_ar).strip()[:1500]
    low_clean = str(low_ar).strip()[:1500]

    prompt = CONSISTENCY_PROMPT_TEMPLATE.format(
        high_ar=high_clean,
        low_ar=low_clean
    )

    for attempt in range(3):
        try:
            with semaphore:
                response = CLIENT.chat.completions.create(
                    model="qwen-max",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,  # 降低随机性，提高稳定性
                    timeout=30
                )
            raw_output = response.choices[0].message.content.strip()

            # 尝试提取并解析 JSON
            json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
            if json_match:
                try:
                    result_dict = json.loads(json_match.group())
                    if result_dict.get("result") == "通过":
                        return "通过"
                    else:
                        reason = result_dict.get("reason", "未提供具体原因")
                        return f"不通过：{reason}"
                except json.JSONDecodeError:
                    pass  # 解析失败，走 fallback

            # Fallback：若包含“通过”且无“不通过”，视为通过
            if "通过" in raw_output and "不通过" not in raw_output:
                return "通过"
            else:
                # 提取可能的原因描述
                clean_reason = re.sub(r"[{}]\"\'\n\r]", "", raw_output)
                return f"不通过：{clean_reason[:200]}..."

        except Exception as e:
            if attempt == 2:
                return f"检测失败：LLM 调用异常（{str(e)[:100]}）"
            time.sleep(2)

    return "检测失败：重试次数耗尽"


def process_file(filepath: str):
    """处理单个 Excel 文件的一致性检测"""
    filename = os.path.basename(filepath)
    print(f"\n[INFO] 正在检测: {filename}")

    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f"  ✘ 读取失败: {e}")
        return

    required_cols = ["AR_高级需求", "LLM_AR_细节需求"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"  ⚠ 跳过：缺少必要列 {missing}")
        return

    results = []
    total_rows = len(df)
    for idx, row in df.iterrows():
        high_ar = row["AR_高级需求"]
        low_ar = row["LLM_AR_细节需求"]
        check_result = call_consistency_check(high_ar, low_ar)
        results.append(check_result)

        if (idx + 1) % 5 == 0:
            print(f"    已完成 {idx + 1}/{total_rows} 行")

    df["一致性检测结果"] = results

    # 生成输出文件名（替换前缀）
    output_filename = filename.replace("decomposed_requirements_", "checked_decomposed_requirements_", 1)
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    try:
        df.to_excel(output_path, index=False, engine="openpyxl")
        print(f"  ✓ 检测完成！结果已保存至: {output_path}")
    except Exception as e:
        print(f"  ✘ 保存失败: {e}")


def main():
    """主函数：批量处理所有文件"""
    # 自动创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 扫描输入文件
    input_files = [
        f for f in os.listdir(INPUT_DIR)
        if f.endswith(".xlsx")
           and not f.startswith("~$")  # 跳过 Excel 临时文件
           and f.startswith("decomposed_requirements_")
    ]

    if not input_files:
        raise FileNotFoundError(
            f"在目录 '{INPUT_DIR}' 中未找到符合 'decomposed_requirements_*.xlsx' 命名规范的文件"
        )

    print(f"[INFO] 共发现 {len(input_files)} 个待检测文件")

    for filename in input_files:
        filepath = os.path.join(INPUT_DIR, filename)
        process_file(filepath)

    print(f"\n🎉 所有文件一致性检测已完成！结果保存在目录: '{OUTPUT_DIR}/'")


if __name__ == "__main__":
    main()