#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日环比波动分析报告生成脚本
功能：读取每日更新数据集，计算各维度环比波动，生成结构化分析报告
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import requests
import markdown
import os

sys.path.insert(0, "/Volumes/system/pypro")
from guandata_client import GuanDataFetcher, FilterCondition, get_token


def load_and_preprocess_data(df):
    """加载并预处理数据"""
    # 读取CSV文件
    # df = pd.read_csv(file_path, encoding="utf-8")

    # 数值列转换为浮点型
    numeric_cols = ["计费金额", "成本金额", "毛利_new", "成本带宽G", "计费带宽G"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 处理统计日期，提取日期部分
    df["统计日期"] = pd.to_datetime(df["统计日期"]).dt.date
    # 统计日期-1天，还原数据真实时间
    df["统计日期"] = df["统计日期"] - pd.Timedelta(days=1)
    return df


def get_compare_dates(df):
    """获取最新日期和对比日期（前一天）"""
    dates = sorted(df["统计日期"].unique())
    latest_date = dates[-1]
    prev_date = dates[-2]
    return latest_date, prev_date


def calculate_overall_metrics(df, latest_date, prev_date):
    """计算整体汇总指标和环比"""
    # 按日期聚合
    daily_agg = (
        df.groupby("统计日期")[["计费金额", "成本金额", "毛利_new"]].sum().reset_index()
    )

    # 获取两个日期的数据
    latest_data = daily_agg[daily_agg["统计日期"] == latest_date].iloc[0]
    prev_data = daily_agg[daily_agg["统计日期"] == prev_date].iloc[0]

    # 计算环比
    overall_result = {
        "latest_date": latest_date,
        "prev_date": prev_date,
        "计费金额_latest": latest_data["计费金额"],
        "计费金额_prev": prev_data["计费金额"],
        "计费金额_diff": latest_data["计费金额"] - prev_data["计费金额"],
        "计费金额_rate": (latest_data["计费金额"] - prev_data["计费金额"])
        / prev_data["计费金额"]
        * 100
        if prev_data["计费金额"] != 0
        else np.inf,
        "成本金额_latest": latest_data["成本金额"],
        "成本金额_prev": prev_data["成本金额"],
        "成本金额_diff": latest_data["成本金额"] - prev_data["成本金额"],
        "成本金额_rate": (latest_data["成本金额"] - prev_data["成本金额"])
        / prev_data["成本金额"]
        * 100
        if prev_data["成本金额"] != 0
        else np.inf,
        "毛利_latest": latest_data["毛利_new"],
        "毛利_prev": prev_data["毛利_new"],
        "毛利_diff": latest_data["毛利_new"] - prev_data["毛利_new"],
        "毛利_rate": (latest_data["毛利_new"] - prev_data["毛利_new"])
        / prev_data["毛利_new"]
        * 100
        if prev_data["毛利_new"] != 0
        else np.inf,
    }

    return overall_result


def analyze_dimension(
    df,
    latest_date,
    prev_date,
    group_cols,
    top_n=20,
    diff_threshold=10000,
    rate_threshold=10,
    filter_customers=None,
):
    """
    按指定维度分析环比波动
    :param group_cols: 分组列名列表
    :param top_n: 返回TOP N
    :param diff_threshold: 变化绝对值阈值（默认10万）
    :param rate_threshold: 变化率阈值（默认±10%）
    :param filter_customers: 可选，只筛选指定客户列表的数据
    """
    # 筛选两个日期的数据
    df_latest = df[df["统计日期"] == latest_date]
    df_prev = df[df["统计日期"] == prev_date]

    # 如果指定了客户过滤，先过滤数据
    if filter_customers is not None and len(filter_customers) > 0:
        df_latest = df_latest[df_latest["客户_new"].isin(filter_customers)]
        df_prev = df_prev[df_prev["客户_new"].isin(filter_customers)]

    # 按维度聚合，新增带宽指标
    agg_cols = ["计费金额", "成本金额", "毛利_new", "计费带宽G", "成本带宽G"]
    agg_latest = df_latest.groupby(group_cols)[agg_cols].sum().reset_index()
    agg_prev = df_prev.groupby(group_cols)[agg_cols].sum().reset_index()

    # 合并数据
    merged = pd.merge(
        agg_prev, agg_latest, on=group_cols, how="outer", suffixes=("_prev", "_latest")
    ).fillna(0)

    # 计算变化
    merged["计费金额_diff"] = merged["计费金额_latest"] - merged["计费金额_prev"]
    merged["计费金额_rate"] = merged.apply(
        lambda x: (
            x["计费金额_diff"] / x["计费金额_prev"] * 100
            if x["计费金额_prev"] != 0
            else (np.inf if x["计费金额_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["成本金额_diff"] = merged["成本金额_latest"] - merged["成本金额_prev"]
    merged["成本金额_rate"] = merged.apply(
        lambda x: (
            x["成本金额_diff"] / x["成本金额_prev"] * 100
            if x["成本金额_prev"] != 0
            else (np.inf if x["成本金额_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["毛利_diff"] = merged["毛利_new_latest"] - merged["毛利_new_prev"]
    merged["毛利_rate"] = merged.apply(
        lambda x: (
            x["毛利_diff"] / x["毛利_new_prev"] * 100
            if x["毛利_new_prev"] != 0
            else (np.inf if x["毛利_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["计费带宽_diff"] = merged["计费带宽G_latest"] - merged["计费带宽G_prev"]
    merged["成本带宽_diff"] = merged["成本带宽G_latest"] - merged["成本带宽G_prev"]

    # 筛选符合条件的记录
    filter_mask = (abs(merged["毛利_rate"]) >= rate_threshold) | (
        abs(merged["毛利_diff"]) >= diff_threshold
    )

    filtered = merged[filter_mask].copy()

    # 按毛利变化绝对值降序排序
    filtered["毛利_diff_abs"] = abs(filtered["毛利_diff"])
    filtered = (
        filtered.sort_values("毛利_diff_abs", ascending=False)
        .head(top_n)
        .drop("毛利_diff_abs", axis=1)
    )

    return filtered


def generate_markdown_report(
    overall_result,
    customer_result,
    business_result,
    business_item_result,
    output_path="环比波动分析报告.md",
):
    """生成Markdown格式的分析报告"""
    report_content = f"""# 日环比波动分析报告（剔除302）
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
分析周期: {overall_result["prev_date"]} (前一天) → {overall_result["latest_date"]} (最新)

---

## 一、整体环比概览
"""
    # 整体指标
    if overall_result["毛利_diff"] > 0:
        overall_icon = "↑"
        overall_color = "**"
    else:
        overall_icon = "↓"
        overall_color = "**"

    # 毛利颜色设置：增长红色，下降绿色
    profit_color = "red" if overall_result["毛利_diff"] > 0 else "green"
    report_content += f"""
总毛利: {overall_color}{overall_icon} <font color="{profit_color}">{"+" if overall_result["毛利_diff"] > 0 else ""}{overall_result["毛利_diff"]:,.2f}</font>{overall_color} （{overall_result["毛利_prev"]:,.2f} → {overall_result["毛利_latest"]:,.2f}）, 环比<font color="{profit_color}">{"+" if overall_result["毛利_rate"] > 0 else ""}{overall_result["毛利_rate"]:.2f}%</font>
计费金额: {"+" if overall_result["计费金额_diff"] > 0 else ""}{overall_result["计费金额_diff"]:,.2f}, 环比{"+" if overall_result["计费金额_rate"] > 0 else ""}{overall_result["计费金额_rate"]:.2f}%
成本金额: {"+" if overall_result["成本金额_diff"] > 0 else ""}{overall_result["成本金额_diff"]:,.2f}, 环比{"+" if overall_result["成本金额_rate"] > 0 else ""}{overall_result["成本金额_rate"]:.2f}%

---

## 二、客户维度波动（仅毛利变化≥5万）
"""
    if len(customer_result) == 0:
        report_content += "👉 暂无符合毛利≥5万的客户波动记录\n"
    else:
        for idx, row in customer_result.iterrows():
            icon = "↑" if row["毛利_diff"] > 0 else "↓"
            highlight = "**" if abs(row["毛利_diff"]) > 200000 else ""
            row_profit_color = "red" if row["毛利_diff"] > 0 else "green"
            report_content += f'{idx + 1}. {icon} {row["客户_new"]:<15} 毛利: {highlight}<font color="{row_profit_color}">{"+" if row["毛利_diff"] > 0 else ""}{row["毛利_diff"]:,.2f}</font>{highlight}（<font color="{row_profit_color}">{"+" if row["毛利_rate"] > 0 else ""}{row["毛利_rate"]:.2f}%</font>）\n'
            report_content += f"  计费金额: {'+' if row['计费金额_diff'] > 0 else ''}{row['计费金额_diff']:,.2f} | 成本金额: {'+' if row['成本金额_diff'] > 0 else ''}{row['成本金额_diff']:,.2f}\n"
            report_content += f"  带宽: 计费带宽{'+' if row['计费带宽_diff'] > 0 else ''}{row['计费带宽_diff']:.2f}G | 成本带宽{'+' if row['成本带宽_diff'] > 0 else ''}{row['成本带宽_diff']:.2f}G\n\n"

    report_content += """
---

## 三、业务方维度波动
"""
    if len(business_result) == 0:
        report_content += "👉 暂无符合变化率≥10%的业务方波动记录\n"
    else:
        for idx, row in business_result.iterrows():
            icon = "↑" if row["毛利_diff"] > 0 else "↓"
            highlight = "**" if abs(row["毛利_diff"]) > 200000 else ""
            row_profit_color = "red" if row["毛利_diff"] > 0 else "green"
            report_content += f'{idx + 1}. {icon} {row["业务方"]:<20} 毛利: {highlight}<font color="{row_profit_color}">{"+" if row["毛利_diff"] > 0 else ""}{row["毛利_diff"]:,.2f}</font>{highlight}（<font color="{row_profit_color}">{"+" if row["毛利_rate"] > 0 else ""}{row["毛利_rate"]:.2f}%</font>）\n'
            report_content += f"  计费金额: {'+' if row['计费金额_diff'] > 0 else ''}{row['计费金额_diff']:,.2f} | 成本金额: {'+' if row['成本金额_diff'] > 0 else ''}{row['成本金额_diff']:,.2f}\n"
            report_content += f"  带宽: 计费带宽{'+' if row['计费带宽_diff'] > 0 else ''}{row['计费带宽_diff']:.2f}G | 成本带宽{'+' if row['成本带宽_diff'] > 0 else ''}{row['成本带宽_diff']:.2f}G\n\n"

    report_content += """
---

## 四、业务方+计费项维度明细
"""
    if len(business_item_result) == 0:
        report_content += "👉 暂无符合阈值的业务方+计费项波动记录\n"
    else:
        for idx, row in business_item_result.iterrows():
            rate = row["毛利_rate"]
            # 修复毛利从负转正导致的异常负变化率
            if row["毛利_new_prev"] < 0 and row["毛利_new_latest"] > 0:
                rate = abs(rate)

            icon = "↑" if row["毛利_diff"] > 0 else "↓"
            highlight = "**" if abs(row["毛利_diff"]) > 100000 else ""
            row_profit_color = "red" if row["毛利_diff"] > 0 else "green"

            report_content += (
                f"{idx + 1}. {icon} {row['业务方']} | {row['业务侧计费项']}\n"
            )
            report_content += f'  毛利: {highlight}<font color="{row_profit_color}">{"+" if row["毛利_diff"] > 0 else ""}{row["毛利_diff"]:,.2f}</font>{highlight}（<font color="{row_profit_color}">{"+" if rate > 0 else ""}{rate:.2f}%</font>）\n'
            report_content += f"  计费金额: {'+' if row['计费金额_diff'] > 0 else ''}{row['计费金额_diff']:,.2f} | 成本金额: {'+' if row['成本金额_diff'] > 0 else ''}{row['成本金额_diff']:,.2f}\n"
            report_content += f"  带宽: 计费带宽{'+' if row['计费带宽_diff'] > 0 else ''}{row['计费带宽_diff']:.2f}G | 成本带宽{'+' if row['成本带宽_diff'] > 0 else ''}{row['成本带宽_diff']:.2f}G\n\n"

    report_content += f"""
---

## 五、核心结论
1. 整体毛利环比**{"增长" if overall_result["毛利_diff"] > 0 else "下降"}** {"+" if overall_result["毛利_diff"] > 0 else ""}{overall_result["毛利_diff"]:,.2f}，增幅{"+" if overall_result["毛利_rate"] > 0 else ""}{overall_result["毛利_rate"]:.2f}%
{f"2. 客户维度最大波动来自：**{customer_result.iloc[0]['客户_new']}**，贡献{'+' if customer_result.iloc[0]['毛利_diff'] > 0 else ''}{customer_result.iloc[0]['毛利_diff']:,.2f}" if len(customer_result) > 0 else "2. 客户维度无符合阈值（≥5万）的波动记录"}
{f"3. 业务方维度最大波动来自：**{business_result.iloc[0]['业务方']}**，贡献{'+' if business_result.iloc[0]['毛利_diff'] > 0 else ''}{business_result.iloc[0]['毛利_diff']:,.2f}" if len(business_result) > 0 else "3. 业务方维度无符合阈值（变化率≥10%）的波动记录"}
{f"4. 最细粒度最大波动来自：**{business_item_result.iloc[0]['业务方']} + {business_item_result.iloc[0]['业务侧计费项']}**，贡献{'+' if business_item_result.iloc[0]['毛利_diff'] > 0 else ''}{business_item_result.iloc[0]['毛利_diff']:,.2f}" if len(business_item_result) > 0 else "4. 业务方+计费项维度无符合阈值的波动记录"}
"""

    # 写入文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"报告已生成: {output_path}")
    return report_content


def send_to_wechat_webhook(webhook_url, markdown_content, report_path):
    """推送Markdown报告到企业微信机器人，超长自动推送核心结论+附件文件"""
    headers = {"Content-Type": "application/json"}
    MAX_MARKDOWN_LEN = 4000  # 企业微信markdown最大长度限制4096字节，留余量
    webhook_key = webhook_url.split("key=")[-1]

    # 判断内容长度（按字节数计算，中文占3字节）
    if len(markdown_content.encode("utf-8")) <= MAX_MARKDOWN_LEN:
        # 内容不超长，直接推送全文
        payload = {"msgtype": "markdown", "markdown": {"content": markdown_content}}
        push_type = "全文"
        try:
            response = requests.post(
                webhook_url, json=payload, headers=headers, timeout=10
            )
            response_data = response.json()
            if response_data.get("errcode") == 0:
                print(f"✅ 报告已成功推送至企业微信（{push_type}）")
                return True
            else:
                print(
                    f"❌ 推送失败，错误信息：{response_data.get('errmsg', '未知错误')}"
                )
                return False
        except Exception as e:
            print(f"❌ 推送异常：{str(e)}")
            return False
    else:
        # 内容超长：1. 上传文件到企业微信临时素材 2. 推送核心结论+文件附件
        print("ℹ️  报告内容超长，将推送核心结论+完整报告附件")
        try:
            # 第一步：将markdown转为带样式的HTML文件
            html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>日环比波动分析报告</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; padding: 20px; max-width: 1200px; margin: 0 auto; }}
        h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f8f9fa; }}
        font[color="red"] {{ color: #e74c3c !important; font-weight: bold; }}
        font[color="green"] {{ color: #27ae60 !important; font-weight: bold; }}
    </style>
</head>
<body>
{}
</body>
</html>
            """
            # markdown转html，支持表格等扩展
            html_body = markdown.markdown(
                markdown_content, extensions=["tables", "extra"]
            )
            html_content = html_template.format(html_body)
            # 保存HTML文件
            html_path = report_path.replace(".md", ".html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            # 第二步：上传HTML文件到企业微信临时素材
            upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={webhook_key}&type=file"
            files = {"media": open(html_path, "rb")}
            upload_resp = requests.post(upload_url, files=files, timeout=15)
            upload_data = upload_resp.json()
            if upload_data.get("errcode") != 0:
                print(f"❌ 文件上传失败：{upload_data.get('errmsg', '未知错误')}")
                os.remove(html_path)  # 清理临时文件
                return False
            media_id = upload_data["media_id"]
            os.remove(html_path)  # 上传成功后清理临时HTML文件

            # 第二步：推送核心结论
            overall_part = ""
            conclusion_part = ""
            sections = markdown_content.split("---")
            if len(sections) >= 1:
                overall_part = sections[0].strip() + "\n\n"
            if len(sections) >= 4:
                conclusion_part = sections[-1].strip()

            push_content = f"""# 日环比波动分析报告（超长精简版）
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
{overall_part}
{conclusion_part}

> 📎 完整报告见下方附件
"""
            # 先推核心结论
            payload = {"msgtype": "markdown", "markdown": {"content": push_content}}
            resp1 = requests.post(
                webhook_url, json=payload, headers=headers, timeout=10
            )

            # 再推文件附件
            payload_file = {"msgtype": "file", "file": {"media_id": media_id}}
            resp2 = requests.post(
                webhook_url, json=payload_file, headers=headers, timeout=10
            )

            if resp1.json().get("errcode") == 0 and resp2.json().get("errcode") == 0:
                print("✅ 报告已成功推送至企业微信（核心结论+完整附件）")
                return True
            else:
                print(
                    f"❌ 推送失败：{resp1.json().get('errmsg', '')} {resp2.json().get('errmsg', '')}"
                )
                return False

        except Exception as e:
            print(f"❌ 推送异常：{str(e)}")
            return False


if __name__ == "__main__":
    # 配置
    from datetime import datetime
    today = datetime.today()
    # 如果当前日期大于等于3号，则使用本月，否则使用上月
    if today.day >= 3:
        current_month = f"{today.year}-{today.month:02d}"
    else:
        # 上个月
        if today.month == 1:
            current_month = f"{today.year - 1}-12"
        else:
            current_month = f"{today.year}-{today.month - 1:02d}"
    
    FILE_PATH = "/Users/nany/Downloads/历史每日最末更新数据集 2026-03-16 09_52_19.csv"
    fc = FilterCondition()
    fc.eq("合并月份", current_month).ne("客户_new", "七牛CDN")
    token = get_token()
    result2 = GuanDataFetcher.fetch_data(
        token=token, ds_id="eff95e2a2fe0048dfb9727b1", filter_condition=fc, limit=50000
    )
    col = result2.get("columns", [])
    colnames = [col[i]["name"] for i in range(len(col))]
    df2 = pd.DataFrame(result2.get("preview", []), columns=colnames)
    OUTPUT_REPORT_PATH = "/home/yanzhuxin/guany/reports/环比波动分析报告.md"

    # 1. 加载数据
    print("正在加载数据...")
    df = load_and_preprocess_data(df2)

    # 2. 获取对比日期
    latest_date, prev_date = get_compare_dates(df)
    print(f"分析日期: {prev_date} → {latest_date}（前一天→最新）")

    # 3. 计算整体指标
    print("计算整体指标...")
    overall_result = calculate_overall_metrics(df, latest_date, prev_date)

    # 4. 各维度独立分析：所有维度均按毛利≥5万阈值筛选，互不依赖
    print("分析客户维度...")
    # 客户维度：毛利变化≥5万
    customer_dim = analyze_dimension(
        df, latest_date, prev_date, ["客户_new"], top_n=10, diff_threshold=50000
    )
    # 二次严格过滤，只保留毛利变动≥5万的客户
    customer_dim = customer_dim[abs(customer_dim["毛利_diff"]) >= 50000].reset_index(
        drop=True
    )

    print("分析业务方维度...")
    # 业务方维度：毛利变化≥5万（全量分析，不依赖客户维度结果，不用变化率阈值）
    business_dim = analyze_dimension(
        df,
        latest_date,
        prev_date,
        ["业务方"],
        top_n=10,
        diff_threshold=50000,
        rate_threshold=0,  # 禁用变化率过滤，仅用金额阈值
    ).reset_index(drop=True)
    # 二次过滤：仅保留毛利变化绝对值≥5万的记录
    business_dim = business_dim[abs(business_dim["毛利_diff"]) >= 50000].reset_index(
        drop=True
    )

    # print("分析业务方+计费项维度...")
    # # 业务方+计费项维度：毛利变化≥5万（全量分析，不依赖上层结果）
    # business_item_dim = analyze_dimension(
    #     df,
    #     latest_date,
    #     prev_date,
    #     ["业务方", "业务侧计费项"],
    #     top_n=10,
    #     diff_threshold=50000,
    #     rate_threshold=0,  # 禁用变化率过滤，仅用金额阈值
    # ).reset_index(drop=True)
    # # 二次过滤：仅保留毛利变化绝对值≥5万的记录
    # business_item_dim = business_item_dim[
    #     abs(business_item_dim["毛利_diff"]) >= 50000
    # ].reset_index(drop=True)

    # 5. 生成报告
    print("生成分析报告...")
    report = generate_markdown_report(
        overall_result,
        customer_dim,
        business_dim,
        pd.DataFrame(),
        OUTPUT_REPORT_PATH,
     )

    # 推送至企业微信机器人
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0d04ba7b-40e4-4502-bcce-bcbc60a2bfd4"
    WECHAT_WEBHOOK_TEST = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595"
    USE_TEST_WEBHOOK = False  # 使用测试webhook
    if USE_TEST_WEBHOOK:
        print("ℹ️  使用测试webhook推送")
        send_to_wechat_webhook(WECHAT_WEBHOOK_TEST, report, OUTPUT_REPORT_PATH)
    else:
        print("ℹ️  使用正式webhook推送")
        send_to_wechat_webhook(WECHAT_WEBHOOK, report, OUTPUT_REPORT_PATH)

    # 打印核心结论
    print("\n===== 核心结论 =====")
    print(
        f"总毛利环比变化: {'+' if overall_result['毛利_diff'] > 0 else ''}{overall_result['毛利_diff']:,.2f} ({'+' if overall_result['毛利_rate'] > 0 else ''}{overall_result['毛利_rate']:.2f}%)"
    )
    print(
        f"最大波动项: {business_dim.iloc[0]['业务方']}, 毛利变化{'+' if business_dim.iloc[0]['毛利_diff'] > 0 else ''}{business_dim.iloc[0]['毛利_diff']:,.2f}"
    )
