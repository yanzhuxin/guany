#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一体化资源波动分析报告生成脚本
功能：自动获取指定数据集、多条件筛选、资源分类、波动分析、生成报告全流程
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import markdown
import os

sys.path.insert(0, "/Volumes/system/pypro")
from guandata_client import GuanDataFetcher, FilterCondition, get_token


def extract_core_conclusion(markdown_content):
    """提取核心结论部分"""
    lines = markdown_content.split('\n')
    core_conclusion = []
    in_core = False
    
    # 提取整体波动概览和核心结论
    for line in lines:
        if "## 二、整体波动概览" in line:
            in_core = True
        elif "---" in line and in_core:
            core_conclusion.append(line)
            break
        elif in_core:
            core_conclusion.append(line)
    
    # 添加核心结论部分
    found_core = False
    for line in lines:
        if "## 三、核心结论" in line:
            found_core = True
        if found_core:
            core_conclusion.append(line)
            if line.strip() == "":
                break
    
    return '\n'.join(core_conclusion)

def send_to_wechat_webhook(webhook_url, markdown_content, report_path):
    """根据内容大小选择推送方式：小于4000字节直接推送，大于则推送结论+HTML文件"""
    webhook_key = webhook_url.split("key=")[-1]
    content_size = len(markdown_content.encode('utf-8'))
    threshold = 4000  # 字节阈值
    
    try:
        if content_size <= threshold:
            # 小于等于4000字节，直接推送markdown文本
            print(f"ℹ️  报告内容大小{content_size}字节 <= {threshold}字节，直接推送完整内容")
            send_url = webhook_url
            headers = {"Content-Type": "application/json"}
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": markdown_content
                }
            }
            send_resp = requests.post(send_url, json=payload, headers=headers, timeout=10)
            send_data = send_resp.json()
            
            if send_data.get("errcode") == 0:
                print("✅ 完整报告已成功推送至企业微信")
                return True
            else:
                print(f"❌ 推送失败：{send_data.get('errmsg', '未知错误')}")
                return False
        else:
            # 大于4000字节，推送核心结论 + HTML文件附件
            print(f"ℹ️  报告内容大小{content_size}字节 > {threshold}字节，推送核心结论+HTML文件")
            
            # 先推送核心结论
            core_content = extract_core_conclusion(markdown_content)
            send_url = webhook_url
            headers = {"Content-Type": "application/json"}
            
            # 推送核心结论文本
            core_payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": core_content + "\n\n> 完整报告过大，已作为附件推送如下"
                }
            }
            core_resp = requests.post(send_url, json=core_payload, headers=headers, timeout=10)
            core_data = core_resp.json()
            
            if core_data.get("errcode") != 0:
                print(f"❌ 核心结论推送失败：{core_data.get('errmsg', '未知错误')}")
            
            # 上传完整HTML报告文件
            temp_file_path = (
                f"/tmp/资源波动分析报告_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
            )
            
            # 将Markdown转换为完整HTML
            html_body = markdown.markdown(
                markdown_content, extensions=["tables", "extra"]
            )
            html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>资源维度波动分析报告</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; padding: 20px; max-width: 1200px; margin: 0 auto; }
        h1, h2 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        table { border-collapse: collapse; width: 100%; margin: 15px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f8f9fa; }
        font[color="red"] { color: #e74c3c !important; font-weight: bold; }
        font[color="green"] { color: #27ae60 !important; font-weight: bold; }
    </style>
</head>
<body>
{}
</body>
</html>
            """
            html_content = html_template.format(html_body)
            with open(temp_file_path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={webhook_key}&type=file"
            files = {"media": open(temp_file_path, "rb")}
            upload_resp = requests.post(upload_url, files=files, timeout=15)
            upload_data = upload_resp.json()
            
            if upload_data.get("errcode") != 0:
                print(f"❌ 文件上传失败：{upload_data.get('errmsg', '未知错误')}")
                return False
                
            media_id = upload_data["media_id"]
            
            # 推送文件附件
            payload = {"msgtype": "file", "file": {"media_id": media_id}}
            send_resp = requests.post(send_url, json=payload, headers=headers, timeout=10)
            send_data = send_resp.json()
            
            if send_data.get("errcode") == 0:
                print("✅ 核心结论和HTML报告已成功推送至企业微信")
                return True
            else:
                print(f"❌ 文件推送失败：{send_data.get('errmsg', '未知错误')}")
                return False
             
    except Exception as e:
        print(f"❌ 推送异常：{str(e)}")
        return False
    finally:
        # 清理临时文件
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


if __name__ == "__main__":
    # 配置参数
    TARGET_DS_ID = "m5ee9ddb4ec7c4fc19fecfac"
    OUTPUT_DIR = "/home/yanzhuxin/guany/reports/"
    OUTPUT_REPORT_PATH = OUTPUT_DIR + "不含七牛CDN_302资源类型波动分析报告.md"
    OUTPUT_DATA_PATH = OUTPUT_DIR + "不含七牛CDN_302分析数据集.csv"
    # 企业微信推送配置（开启后推送HTML附件）
    ENABLE_WECHAT_PUSH = True
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0d04ba7b-40e4-4502-bcce-bcbc60a2bfd4"
    WECHAT_WEBHOOK_TEST = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595"
    USE_TEST_WEBHOOK = False  # 切换到正式webhook

    # ========== 1. 数据获取阶段 ==========
    print("===== 开始数据获取 =====")

    # 计算昨天凌晨时间
    yesterday_midnight = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    print(f"时间筛选条件：大于 {yesterday_midnight.strftime('%Y-%m-%d %H:%M:%S')}")

    # 动态计算筛选月份：当月3号及以后选本月，否则选上月
    today = datetime.now()
    if today.day >= 3:
        filter_month = today.strftime("%Y-%m")
    else:
        # 计算上月
        last_month = today.replace(day=1) - timedelta(days=1)
        filter_month = last_month.strftime("%Y-%m")
    print(f"筛选月份：{filter_month}")

    # 构建API筛选条件
    fc = FilterCondition()
    fc.eq("月份", filter_month)
    fc.gt("时间", yesterday_midnight.strftime("%Y-%m-%d %H:%M:%S"))

    # 获取数据
    print("正在从API获取数据集...")
    token = get_token()
    result = GuanDataFetcher.fetch_data(
        token=token,
        ds_id=TARGET_DS_ID,
        filter_condition=fc,
        limit=100000,
    )

    # 处理返回结果
    col = result.get("columns", [])
    colnames = [col[i]["name"] for i in range(len(col))]
    df = pd.DataFrame(result.get("preview", []), columns=colnames)

    # 打印数据集信息
    print("="*60)
    print(f"数据集ID: {TARGET_DS_ID}")
    print(f"数据集名称: 资源类型分析数据集")
    print(f"字段总数: {len(colnames)}")
    print("字段列表:")
    for i, field in enumerate(colnames, 1):
        print(f"  {i}. {field}")
    print("="*60)
    print(f"原始数据行数: {len(df)} 行")

    # 签约方名称过滤：不包含七牛和302
    df = df[
        ~df["签约方名称"].fillna("").str.contains("七牛|302", case=False, na=False)
    ].reset_index(drop=True)
    print(f"签约方过滤后剩余数据：{len(df)} 行")

    # 时间快照校验与处理
    df["时间"] = pd.to_datetime(df["时间"])
    unique_times = sorted(df["时间"].unique())
    print(
        f"原始数据包含 {len(unique_times)} 个时间快照：{[t.strftime('%Y-%m-%d %H:%M:%S') for t in unique_times]}"
    )

    if len(unique_times) != 2:
        print("⚠️  时间快照数量不等于2，自动取每天最后一次快照")
        # 提取日期维度
        df["日期"] = df["时间"].dt.date
        # 按日期分组取最大时间（每天最后一次快照）
        daily_last_snapshot = df.groupby("日期")["时间"].max().reset_index()
        keep_times = daily_last_snapshot["时间"].tolist()
        # 过滤保留目标时间点数据
        df = df[df["时间"].isin(keep_times)].reset_index(drop=True)
        print(
            f"处理后保留 {len(keep_times)} 个时间快照：{[t.strftime('%Y-%m-%d %H:%M:%S') for t in keep_times]}"
        )
        if len(keep_times) < 2:
            print("❌ 有效时间点不足2个，无法进行波动分析")
            exit()

    time_prev, time_latest = unique_times[0], unique_times[1]
    print(f"分析时间点：{time_prev} → {time_latest}")

    # ========== 2. 分析计算阶段 ==========
    print("\n===== 开始分析计算 =====")

    # 数据预处理：数值字段类型转换
    numeric_cols = [
        "系数计量金额",
        "整月成本金额",
        "整月成本带宽",
        "系数计量带宽",
        "整月计量金额",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 计算毛利：系数计量金额 - 整月成本金额
    df["毛利"] = df["系数计量金额"] - df["整月成本金额"]

    # 新增资源分类列
    def classify_resource(row):
        if pd.notna(row["节点类型"]) and row["节点类型"] == "smallBox":
            return "盒子"
        elif pd.notna(row["节点类型"]) and row["节点类型"] == "switch":
            return "专线"
        elif pd.notna(row["resourceType"]) and row["resourceType"] == "汇聚":
            return "汇聚"
        elif pd.notna(row["resourceType"]) and row["resourceType"] == "专线":
            return "专线"
        else:
            return "其他"

    df["资源分类"] = df.apply(classify_resource, axis=1)

    # 整体波动分析
    total_prev = df[df["时间"] == time_prev]["毛利"].sum()
    total_latest = df[df["时间"] == time_latest]["毛利"].sum()
    total_diff = total_latest - total_prev
    total_rate = (total_diff / total_prev * 100) if total_prev != 0 else np.inf

    # 资源类型维度波动分析
    agg_columns = [
        "系数计量金额",
        "整月成本金额",
        "整月成本带宽",
        "系数计量带宽",
    ]
    resource_agg = df.groupby(["时间", "资源分类"])[agg_columns].sum().reset_index()

    # 分开两个时间点的数据
    prev_data = resource_agg[resource_agg["时间"] == time_prev].set_index("资源分类")
    latest_data = resource_agg[resource_agg["时间"] == time_latest].set_index(
        "资源分类"
    )

    # 合并数据
    resource_result = pd.merge(
        prev_data,
        latest_data,
        left_index=True,
        right_index=True,
        how="outer",
        suffixes=("_prev", "_latest"),
    ).fillna(0)

    # 计算毛利：系数计量金额 - 整月成本金额
    resource_result["毛利_prev"] = resource_result["系数计量金额_prev"] - resource_result["整月成本金额_prev"]
    resource_result["毛利_latest"] = resource_result["系数计量金额_latest"] - resource_result["整月成本金额_latest"]

    # 计算波动
    resource_result["毛利_diff"] = (
        resource_result["毛利_latest"] - resource_result["毛利_prev"]
    )
    resource_result["毛利_rate"] = resource_result.apply(
        lambda x: (
            (x["毛利_diff"] / x["毛利_prev"] * 100)
            if x["毛利_prev"] != 0
            else (np.inf if x["毛利_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )
    resource_result["系数计量金额_diff"] = (
        resource_result["系数计量金额_latest"] - resource_result["系数计量金额_prev"]
    )
    resource_result["整月成本金额_diff"] = (
        resource_result["整月成本金额_latest"] - resource_result["整月成本金额_prev"]
    )
    resource_result["整月成本带宽_diff"] = (
        resource_result["整月成本带宽_latest"] - resource_result["整月成本带宽_prev"]
    )
    resource_result["系数计量带宽_diff"] = (
        resource_result["系数计量带宽_latest"] - resource_result["系数计量带宽_prev"]
    )

    # 按毛利波动绝对值排序
    resource_result = resource_result.sort_values("毛利_diff", key=abs, ascending=False)

    # ========== 3. 报告生成阶段 ==========
    print("\n===== 生成分析报告 =====")

    report = f"""# 不含七牛CDN/302资源类型波动分析报告
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
分析周期: {time_prev} → {time_latest}
筛选规则：月份={filter_month}，时间≥昨日凌晨，签约方不含七牛/302

---

## 一、资源分类规则说明
```mermaid
flowchart LR
    A[开始分类] --> B{{节点类型 == smallBox?}}
    B -->|是| C[盒子]
    B -->|否| D{{节点类型 == switch?}}
    D -->|是| E[专线]
    D -->|否| F{{resourceType == 汇聚?}}
    F -->|是| G[汇聚]
    F -->|否| H{{resourceType == 专线?}}
    H -->|是| I[专线]
    H -->|否| J[其他]
```

---

## 二、整体波动概览
总毛利: **{"↑" if total_diff > 0 else "↓"} {total_diff/10000:,.2f}万元** （{total_prev/10000:,.2f}万元 → {total_latest/10000:,.2f}万元）, 环比**{"+" if total_rate > 0 else ""}{total_rate:.2f}%**
系数计量金额: {"+" if (resource_result["系数计量金额_diff"].sum()) > 0 else ""}{resource_result["系数计量金额_diff"].sum()/10000:,.2f}万元
整月成本金额: {"+" if (resource_result["整月成本金额_diff"].sum()) > 0 else ""}{resource_result["整月成本金额_diff"].sum()/10000:,.2f}万元
整月成本带宽: {"+" if (resource_result["整月成本带宽_diff"].sum()) > 0 else ""}{resource_result["整月成本带宽_diff"].sum():,.2f}G
系数计量带宽: {"+" if (resource_result["系数计量带宽_diff"].sum()) > 0 else ""}{resource_result["系数计量带宽_diff"].sum():,.2f}G

---

## 二、资源类型维度波动明细
========================================================================================
资源分类    前值毛利      现值毛利      毛利变化        变化率    金额变化   成本变化    成本带宽    计费带宽
========================================================================================
"""

    for res_type, row in resource_result.iterrows():
        profit_prev = row["毛利_prev"]
        profit_latest = row["毛利_latest"]
        profit_diff = row["毛利_diff"]
        profit_rate = row["毛利_rate"]
        amount_diff = row["系数计量金额_diff"]
        cost_diff = row["整月成本金额_diff"]
        cost_bw_diff = row["整月成本带宽_diff"]
        amount_bw_diff = row["系数计量带宽_diff"]

        icon = "↑" if profit_diff > 0 else "↓"
        color = "red" if profit_diff > 0 else "green"
        # 纯文本对齐，保留颜色格式（企业微信支持font标签）
        profit_diff_str = f"{icon} <font color='{color}'>{'+' if profit_diff > 0 else ''}{profit_diff/10000:.2f}万元</font>"
        report += "%-8s %-12s %-12s %+24s %+8s %+10s %+10s %+10s %+10s\n" % (
            res_type,
            f"{profit_prev/10000:.2f}万元",
            f"{profit_latest/10000:.2f}万元",
            profit_diff_str,
            f"{'+' if profit_rate > 0 else ''}{profit_rate:.2f}%",
            f"{'+' if amount_diff > 0 else ''}{amount_diff/10000:.2f}万",
            f"{'+' if cost_diff > 0 else ''}{cost_diff/10000:.2f}万",
            f"{'+' if cost_bw_diff > 0 else ''}{cost_bw_diff:.2f}G",
            f"{'+' if amount_bw_diff > 0 else ''}{amount_bw_diff:.2f}G"
        )

    # # 资源下钻分析（暂时注释）
    # report += "\n---\n\n## 三、签约方波动明细\n"
    # # 先筛选波动>5万的大波动资源
    # big_diff_resources = resource_result[abs(resource_result["毛利_diff"]) > 50000]
    
    # if len(big_diff_resources) > 0:
    #     # 有大波动资源，逐个下钻
    #     report += "### 大波动资源签约方影响\n"
    #     for res_type, row in big_diff_resources.iterrows():
    #         profit_diff = row["毛利_diff"]
    #         # 下钻到签约方维度
    #         res_data = df[df["资源分类"] == res_type]
    #         # 按时间+签约方聚合
    #         customer_agg = res_data.groupby(["时间", "签约方名称"])[["系数计量金额", "整月成本金额"]].sum().reset_index()
    #         # 分开两个时间点
    #         prev_cust = customer_agg[customer_agg["时间"] == time_prev].set_index("签约方名称")
    #         latest_cust = customer_agg[customer_agg["时间"] == time_latest].set_index("签约方名称")
    #         # 合并计算
    #         cust_result = pd.merge(prev_cust, latest_cust, left_index=True, right_index=True, how="outer", suffixes=("_prev", "_latest")).fillna(0)
    #         cust_result["毛利_prev"] = cust_result["系数计量金额_prev"] - cust_result["整月成本金额_prev"]
    #         cust_result["毛利_latest"] = cust_result["系数计量金额_latest"] - cust_result["整月成本金额_latest"]
    #         cust_result["毛利_diff"] = cust_result["毛利_latest"] - cust_result["毛利_prev"]
    #         # 筛选波动>1万的签约方，按波动绝对值排序
    #         cust_result = cust_result[abs(cust_result["毛利_diff"]) > 10000].sort_values("毛利_diff", key=abs, ascending=False)
            
    #         if len(cust_result) > 0:
    #             report += f"\n#### {res_type}（毛利变化{'+' if profit_diff > 0 else ''}{profit_diff/10000:.2f}万元）\n"
    #             report += "========================================================================================\n"
    #             report += "%-20s %-12s %-12s %+24s %+8s\n" % ("签约方名称", "前值毛利", "现值毛利", "毛利变化", "变化率")
    #             report += "========================================================================================\n"
    #             for cust_name, cust_row in cust_result.iterrows():
    #                 c_profit_prev = cust_row["毛利_prev"]
    #                 c_profit_latest = cust_row["毛利_latest"]
    #                 c_profit_diff = cust_row["毛利_diff"]
    #                 c_profit_rate = (c_profit_diff / c_profit_prev * 100) if c_profit_prev != 0 else (np.inf if c_profit_diff>0 else -np.inf)
    #                 c_icon = "↑" if c_profit_diff > 0 else "↓"
    #                 c_color = "red" if c_profit_diff > 0 else "green"
    #                 c_diff_str = f"{c_icon} <font color='{c_color}'>{'+' if c_profit_diff > 0 else ''}{c_profit_diff/10000:.2f}万元</font>"
    #                 report += "%-20s %-12s %-12s %+24s %+8s\n" % (
    #                     cust_name[:18], 
    #                     f"{c_profit_prev/10000:.2f}万",
    #                     f"{c_profit_latest/10000:.2f}万",
    #                     c_diff_str,
    #                     f"{'+' if c_profit_rate > 0 else ''}{c_profit_rate:.2f}%"
    #                 )
    #             report += "----------------------------------------------------------------------------------------\n"
    # else:
    #     # 无大波动资源，展示每个资源类型下的签约方波动TOP5
    #     report += "### 各资源类型签约方波动TOP5\n"
    #     # 遍历所有资源类型
    #     for res_type in ["专线", "盒子", "汇聚", "其他"]:
    #         res_data = df[df["资源分类"] == res_type]
    #         if len(res_data) == 0:
    #             continue
    #         # 按签约方聚合计算波动
    #         customer_agg = res_data.groupby(["时间", "签约方名称"])[["系数计量金额", "整月成本金额"]].sum().reset_index()
    #         prev_cust = customer_agg[customer_agg["时间"] == time_prev].set_index("签约方名称")
    #         latest_cust = customer_agg[customer_agg["时间"] == time_latest].set_index("签约方名称")
    #         cust_result = pd.merge(prev_cust, latest_cust, left_index=True, right_index=True, how="outer", suffixes=("_prev", "_latest")).fillna(0)
    #         cust_result["毛利_prev"] = cust_result["系数计量金额_prev"] - cust_result["整月成本金额_prev"]
    #         cust_result["毛利_latest"] = cust_result["系数计量金额_latest"] - cust_result["整月成本金额_latest"]
    #         cust_result["毛利_diff"] = cust_result["毛利_latest"] - cust_result["毛利_prev"]
    #         # 按波动绝对值排序取TOP5
    #         cust_result = cust_result.sort_values("毛利_diff", key=abs, ascending=False).head(5)
            
    #         if len(cust_result) > 0:
    #             report += f"\n#### {res_type}\n"
    #             report += "========================================================================================\n"
    #             report += "%-20s %-12s %-12s %+24s %+8s\n" % ("签约方名称", "前值毛利", "现值毛利", "毛利变化", "变化率")
    #             report += "========================================================================================\n"
    #             for cust_name, cust_row in cust_result.iterrows():
    #                 c_profit_prev = cust_row["毛利_prev"]
    #                 c_profit_latest = cust_row["毛利_latest"]
    #                 c_profit_diff = cust_row["毛利_diff"]
    #                 c_profit_rate = (c_profit_diff / c_profit_prev * 100) if c_profit_prev != 0 else (np.inf if c_profit_diff>0 else -np.inf)
    #                 c_icon = "↑" if c_profit_diff > 0 else "↓"
    #                 c_color = "red" if c_profit_diff > 0 else "green"
    #                 c_diff_str = f"{c_icon} <font color='{c_color}'>{'+' if c_profit_diff > 0 else ''}{c_profit_diff/10000:.2f}万元</font>"
    #                 report += "%-20s %-12s %-12s %+24s %+8s\n" % (
    #                     cust_name[:18], 
    #                     f"{c_profit_prev/10000:.2f}万",
    #                     f"{c_profit_latest/10000:.2f}万",
    #                     c_diff_str,
    #                     f"{'+' if c_profit_rate > 0 else ''}{c_profit_rate:.2f}%"
    #                 )
    #             report += "----------------------------------------------------------------------------------------\n"
    #     report += "----------------------------------------------------------------------------------------\n"

    report += f"""
---

## 三、核心结论
1. 整体毛利环比**{"增长" if total_diff > 0 else "下降"}** {total_diff/10000:,.2f}万元，增幅{total_rate:.2f}%
2. 最大波动来自资源类型：**{resource_result.index[0]}**，贡献{"+" if resource_result.iloc[0]["毛利_diff"] > 0 else ""}{resource_result.iloc[0]["毛利_diff"]/10000:,.2f}万元
"""

    # 保存报告
    with open(OUTPUT_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    # 保存分析数据集
    df.to_csv(OUTPUT_DATA_PATH, encoding="utf-8-sig", index=False)

    # 生成HTML版本报告用于展示和推送
    OUTPUT_HTML_PATH = OUTPUT_REPORT_PATH.replace(".md", ".html")
    html_body = markdown.markdown(report, extensions=["tables", "extra"])
    html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>不含七牛CDN_302资源类型波动分析报告</title>
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
    html_content = html_template.format(html_body)
    with open(OUTPUT_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"HTML报告已生成: {OUTPUT_HTML_PATH}")
    
    # 企业微信推送（默认关闭，开启后推送HTML附件）
    if ENABLE_WECHAT_PUSH:
        print("\n正在推送HTML报告到企业微信...")
        if USE_TEST_WEBHOOK:
            print("ℹ️  使用测试webhook推送")
            send_to_wechat_webhook(WECHAT_WEBHOOK_TEST, report, OUTPUT_REPORT_PATH)
        else:
            print("ℹ️  使用正式webhook推送")
            send_to_wechat_webhook(WECHAT_WEBHOOK, report, OUTPUT_REPORT_PATH)

    # ========== 输出结果 ==========
    print(f"\n✅ 分析完成！")
    print(f"分析报告：{OUTPUT_REPORT_PATH}")
    print(f"数据集：{OUTPUT_DATA_PATH}")
    print("\n===== 核心结论 =====")
    print(
        f"总毛利变化: {'+' if total_diff > 0 else ''}{total_diff/10000:,.2f}万元 ({'+' if total_rate > 0 else ''}{total_rate:.2f}%)"
    )
    print(
        f"最大波动资源: {resource_result.index[0]}，毛利变化{'+' if resource_result.iloc[0]['毛利_diff'] > 0 else ''}{resource_result.iloc[0]['毛利_diff']/10000:,.2f}万元"
    )

