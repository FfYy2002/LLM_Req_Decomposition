# -*- coding: utf-8 -*-
"""
需求对转 AR 格式转换脚本

功能说明：
- 读取 clean_output/ 目录下所有 clean_extracted_requirements_*.xlsx 文件
- 对每个文件执行以下流程：
    a) 按“高级需求”字段进行分组
    b) 将同一组内的所有“细节需求”合并为带编号的字符串（格式：1. ...\n2. ...\n3. ...）
    c) 分别对“高级需求”和“合并后的细节需求”调用 Qwen-Max 生成对应的 AR 格式文本
    d) 每个唯一“高级需求”仅保留一行输出，避免重复
- 输出结果保存至 ar_output/ 目录，文件名前缀替换为 "ar_"

作者：minefan
日期：2025年12月10日
"""

import os
import pandas as pd
from openai import OpenAI
import threading
from concurrent.futures import ThreadPoolExecutor
import time


# =============================================================================
# 全局配置
# =============================================================================

INPUT_DIR = "clean_output"
OUTPUT_DIR = "ar_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 从环境变量读取 DashScope API 密钥
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise EnvironmentError(
        "❌ 环境变量 DASHSCOPE_API_KEY 未设置。\n"
        "请在运行前执行：\n"
        "  Windows (PowerShell): $env:DASHSCOPE_API_KEY='sk-xxx'\n"
        "  Linux/macOS: export DASHSCOPE_API_KEY='sk-xxx'"
    )

# 初始化 OpenAI 兼容客户端（DashScope）
CLIENT = OpenAI(
    api_key=DASHSCOPE_API_KEY,
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

# 控制最大并发请求数（防止 API 限流）
MAX_CONCURRENT = 6
semaphore = threading.Semaphore(MAX_CONCURRENT)


# =============================================================================
# 提示词模板
# =============================================================================
AR_PROMPT_TEMPLATE = """你是一位需求分析专家，请将“系统需求”拆分为具体的“开发需求”，即结合系统特点，拆分出其中包含的正常功能场景和边界场景对应的开发需求。在此过程中，严格保持需求边界，不得生成额外内容。

【重要规则】
1. 仅使用系统需求中明确提及或可直接推导的信息：
   - 若需求中提到了具体功能（如 LiveTV、CatchupTV）、接口（如 GetCustomizeConfig）或参数（如 ChannelCategoryID），可合理引用。
   - 若需求仅为简略描述（如 “Add user”、“Display question”），不得自行添加 API、数据库、UI 框架、字段名等未提及内容。
2. 输出必须包含：
   - 正常功能流程（主路径）
   - 至少 2 个边界或异常场景（如数据为空、操作失败、状态冲突等）
3. 输出格式要求：
   - 以编号列表形式呈现（1. 2. 3. …）
   - 可在末尾使用“说明：”补充非功能性要求（如排序、缓存、性能），但仅限原始需求隐含或行业常识（例如“频道按频道号升序排列”）
   - 不得反问，不得请求用户提供更多信息

【示例】
#系统需求：支持LiveTV功能，支持TSTV功能，支持CatchupTV功能，支持频道收藏特性，支持节目提醒特性，支持频道产品在线订购
#开发需求：
1. 客户端主菜单增加 Live TV 入口，用户点击该入口，进入频道列表，默认展示所有频道。
2. 增加 EPG 配置参数ChannelCategoryID用于配置直播频道根栏目，该参数通过 MEM 的 GetCustomizeConfig 接口获取；客户端通过 ChannelList 接口获取频道信息（传入该栏目 ID），并通过 PlayBillContextEx 接口获取当前及下一个节目。
3. 频道列表每项展示：频道图标（取自 picture 对象中的 icon）、频道号、频道名称、当前节目、下一个节目、是否支持 CatchupTV 标识、是否已收藏标识。
4. 用户可按条件过滤频道列表，包括：所有频道、支持 CatchupTV 的频道、已收藏频道、已订购频道，过滤依据为频道对象的相关属性。
说明：
1. 频道列表按频道号升序排列。
2. 频道信息需缓存；当用户执行收藏等操作后，或心跳检测到频道/收藏版本号变更时，应刷新缓存。

#系统需求：{requirement}
#开发需求："""


# =============================================================================
# 调用 LLM 转换单条需求（仅返回原始输出）
# =============================================================================
def convert_to_ar_raw(requirement: str, max_retries: int = 2) -> str:
    """
    将单条自然语言需求转换为 AR 格式，返回 LLM 原始输出（不做清洗）。

    Args:
        requirement (str): 原始需求文本
        max_retries (int): 最大重试次数

    Returns:
        str: LLM 生成的 AR 文本，或错误标记
    """
    if not requirement or pd.isna(requirement) or str(requirement).strip() == "":
        return ""

    prompt = AR_PROMPT_TEMPLATE.format(requirement=str(requirement).strip())

    for attempt in range(max_retries + 1):
        try:
            with semaphore:
                response = CLIENT.chat.completions.create(
                    model="qwen-max",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    stream=False,
                    timeout=30
                )
            result = response.choices[0].message.content.strip()

            # 移除可能存在的 "#开发需求：" 前缀（兼容中英文冒号、有无空格）
            if result.startswith("#开发需求：") or result.startswith("# 开发需求："):
                result = result.split("开发需求：", 1)[-1].strip()
            elif result.startswith("#开发需求:") or result.startswith("# 开发需求:"):
                result = result.split("开发需求:", 1)[-1].strip()

            return result

        except Exception as e:
            if attempt >= max_retries:
                error_msg = f"[LLM_ERROR] {str(e)}"
                print(f"[ERROR] {error_msg} | 原始需求片段: {str(requirement)[:60]}...")
                return error_msg
            else:
                time.sleep(2)
    return "[LLM_ERROR] 未知错误"


# =============================================================================
# 处理单个 Excel 文件
# =============================================================================
def process_single_file(filepath: str) -> pd.DataFrame:
    """
    处理单个清洗后的 Excel 文件，返回去重并合并后的 AR 结果 DataFrame。

    处理逻辑：
    1. 读取 Excel，验证必要列存在
    2. 按“高级需求”分组，合并“细节需求”为编号列表
    3. 并行调用 LLM 生成 AR_高级需求 和 AR_细节需求
    4. 每组仅保留一行输出

    Returns:
        pd.DataFrame: 包含四列的 DataFrame：
            - 原始_高级需求
            - AR_高级需求
            - 原始_细节需求（合并后）
            - AR_细节需求
    """
    df = pd.read_excel(filepath)
    required_cols = ["高级需求", "细节需求"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"文件 {filepath} 缺少必要列: {col}")

    # 去除空值行
    df = df.dropna(subset=required_cols).reset_index(drop=True)

    # Step 1: 按“高级需求”分组，合并“细节需求”为编号字符串
    grouped = (
        df.groupby("高级需求")["细节需求"]
        .apply(lambda x: "\n".join([f"{i+1}. {item}" for i, item in enumerate(x)]))
        .reset_index()
    )
    grouped.rename(columns={"高级需求": "原始_高级需求", "细节需求": "原始_细节需求"}, inplace=True)

    total_groups = len(grouped)
    print(f"\n[INFO] 正在生成 AR: {os.path.basename(filepath)} | 共 {total_groups} 个唯一高级需求")

    # Step 2: 并行调用 LLM 生成 AR 内容
    results = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT * 2) as executor:
        futures = []
        for _, row in grouped.iterrows():
            high_req = row["原始_高级需求"]
            merged_low = row["原始_细节需求"]
            future_high = executor.submit(convert_to_ar_raw, high_req)
            future_low = executor.submit(convert_to_ar_raw, merged_low)
            futures.append((future_high, future_low, row))

        for i, (fh, fl, orig_row) in enumerate(futures):
            ar_high = fh.result()
            ar_low = fl.result()
            results.append({
                "原始_高级需求": orig_row["原始_高级需求"],
                "AR_高级需求": ar_high,
                "原始_细节需求": orig_row["原始_细节需求"],
                "AR_细节需求": ar_low,
            })

            if (i + 1) % 10 == 0 or i + 1 == total_groups:
                print(f"  → 已完成 {i+1}/{total_groups}")

    return pd.DataFrame(results)


# =============================================================================
# 主函数
# =============================================================================
def main():
    """
    主入口函数：
    - 扫描输入目录
    - 逐个处理文件
    - 保存结果到输出目录
    """
    input_files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.startswith("clean_extracted_requirements_") and f.endswith(".xlsx")
    ]

    if not input_files:
        print(f"[FATAL] 在 {INPUT_DIR} 中未找到任何清洗后的需求文件。")
        return

    print(f"[INFO] 发现 {len(input_files)} 个待转换文件")

    for file_path in input_files:
        try:
            ar_df = process_single_file(file_path)
            base_name = os.path.basename(file_path)
            output_name = "ar_" + base_name[len("clean_"):]
            output_path = os.path.join(OUTPUT_DIR, output_name)
            ar_df.to_excel(output_path, index=False, engine="openpyxl")
            print(f"  ✅ 已保存 AR 文件: {output_path}")
        except Exception as e:
            print(f"[SKIP] 跳过文件 {file_path} | 原因: {e}")
            continue

    print(f"\n🎉 所有 {len(input_files)} 个文件的 AR 转换已完成，结果保存在 '{OUTPUT_DIR}' 目录。")


if __name__ == "__main__":
    main()