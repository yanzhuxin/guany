#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
月维度前后两日波动分析报告生成脚本
功能：获取过去12个月数据，对比昨天和今天同一月份的汇总数据，分析月度波动情况，推送到企业微信测试版
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

def load_and_preprocess_data(df):
    """加载并预处理数据"""
    # 数值列转换为浮点型
    numeric_cols = ["计费金额", "成本金额", "毛利_new", "成本带宽G", "计费带宽G"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 处理统计日期，提取日期部分
    df["统计日期"] = pd.to_datetime(df["统计日期"]).dt.date
    # 保持原始日期，已经在数据源做过偏移
    # 提取月份 = 合并月份，所以直接用合并月份
    df["月份"] = df["合并月份"]
    return df


def get_compare_days(df):
    """获取最新两天（昨天和今天）"""
    dates = sorted(df["统计日期"].unique())
    if len(dates) < 2:
        return None, None
    latest_day = dates[-1]
    prev_day = dates[-2]
    return latest_day, prev_day


def calculate_monthly_metrics(df, latest_day, prev_day):
    """按月份计算两天的指标对比"""
    # 分别汇总两天每个月的数据
    latest_agg = df[df["统计日期"] == latest_day].groupby("月份")[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G", "成本带宽G"]
    ].sum().reset_index()

    prev_agg = df[df["统计日期"] == prev_day].groupby("月份")[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G", "成本带宽G"]
    ].sum().reset_index()

    # 合并数据
    merged = pd.merge(
        prev_agg, latest_agg, on="月份", how="outer", suffixes=("_prev", "_latest")
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

    # 获取过去12个月
    current_date = datetime.now()
    months = []
    for i in range(12):
        month = (current_date - timedelta(days=30*i)).strftime("%Y-%m")
        months.append(month)
    
    # 只保留过去12个月，并按毛利变化绝对值降序排序
    merged = merged[merged["月份"].isin(months)]
    merged["毛利_diff_abs"] = abs(merged["毛利_diff"])
    merged = merged.sort_values("毛利_diff_abs", ascending=False).reset_index(drop=True)

    return merged


def calculate_customer_dimension(df, latest_day, prev_day, month):
    """对波动大的月份下钻到客户维度"""
    # 筛选指定月份，分别汇总两天客户数据
    latest_agg = df[(df["统计日期"] == latest_day) & (df["月份"] == month)].groupby(["客户_new"])[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G"]
    ].sum().reset_index()

    prev_agg = df[(df["统计日期"] == prev_day) & (df["月份"] == month)].groupby(["客户_new"])[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G"]
    ].sum().reset_index()

    # 合并数据
    merged = pd.merge(
        prev_agg, latest_agg, on=["客户_new"], how="outer", suffixes=("_prev", "_latest")
    ).fillna(0)

    # 计算变化
    merged["毛利_diff"] = merged["毛利_new_latest"] - merged["毛利_new_prev"]
    merged["毛利_diff_abs"] = abs(merged["毛利_diff"])
    # 只筛选变化绝对值大于1万的客户
    merged = merged[merged["毛利_diff_abs"] > 10000]
    merged = merged.sort_values("毛利_diff_abs", ascending=False).reset_index(drop=True)
    
    return merged

def calculate_business_dimension(df, latest_day, prev_day, month):
    """对波动大的月份下钻到业务方维度"""
    # 筛选指定月份，分别汇总两天业务方数据
    latest_agg = df[(df["统计日期"] == latest_day) & (df["月份"] == month)].groupby(["业务方"])[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G"]
    ].sum().reset_index()

    prev_agg = df[(df["统计日期"] == prev_day) & (df["月份"] == month)].groupby(["业务方"])[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G"]
    ].sum().reset_index()

    # 合并数据
    merged = pd.merge(
        prev_agg, latest_agg, on=["业务方"], how="outer", suffixes=("_prev", "_latest")
    ).fillna(0)

    # 计算变化
    merged["毛利_diff"] = merged["毛利_new_latest"] - merged["毛利_new_prev"]
    merged["毛利_diff_abs"] = abs(merged["毛利_diff"])
    # 只筛选变化绝对值大于1万的业务方
    merged = merged[merged["毛利_diff_abs"] > 10000]
    merged = merged.sort_values("毛利_diff_abs", ascending=False).reset_index(drop=True)
    
    return merged

def generate_markdown_report(df, monthly_result, latest_day, prev_day, output_path):
    """生成Markdown格式的分析报告（适配企业微信，纯文本表格）"""
    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_content = """# 月维度前后两日波动分析报告（排除七牛CDN）
生成时间: {datetime_now}
对比日期: {prev_day} (昨日) → {latest_day} (今日)
筛选条件: 排除七牛CDN，只展示过去12个月数据

========================================================================================
- 整体汇总对比
========================================================================================
指标       昨日汇总            今日汇总            变化金额             变化率
========================================================================================
 """.format(datetime_now=datetime_now, prev_day=prev_day, latest_day=latest_day)

    # 只筛选毛利变化超过3万的大波动月份输出明细
    big_diff_months = monthly_result[abs(monthly_result["毛利_diff"]) > 30000].copy()
    
    if len(big_diff_months) > 0:
        report_content += """
 =======================================================================================
 - 波动月份前后两日指标对比（仅展示毛利波动 > 3万的月份）
 =======================================================================================
 月份    指标           昨日汇总            今日汇总            变化金额             变化率
 =======================================================================================
 """
        # 输出波动月份明细 - 只展示毛利
        for _, row in big_diff_months.iterrows():
            month = row["月份"]
            # 只展示毛利
            report_content += "%s  毛利       %-12.2f万元  %-15.2f万元  %+11.2f万元  %+7.2f%%\n" % (
                month, row["毛利_new_prev"] / 10000, row["毛利_new_latest"] / 10000, 
                row["毛利_diff"] / 10000, row["毛利_rate"]
            )
            report_content += "----------------------------------------------------------------------------------------\n"
    else:
        report_content += """
 =======================================================================================
 - 无波动超过3万的月份
 =======================================================================================
 """

    # 对每个大波动月份，下钻业务方维度
    for _, month_row in big_diff_months.iterrows():
        month = month_row["月份"]
        diff = month_row["毛利_diff"]
        
        # 业务方维度下钻
        business_result = calculate_business_dimension(df, latest_day, prev_day, month)
        if len(business_result) > 0:
            report_content += "\n### {} 月份（毛利变化 {:.2f}万元）\n\n".format(month, diff / 10000)
            report_content += "%-18s %-10s %-10s %-10s %-10s %+10s %+8s\n" % (
                "业务方", "昨日成本", "今日成本", "昨日计费", "今日计费", "毛利变化", "变化率"
            )
            for _, bus_row in business_result.iterrows():
                bname = bus_row["业务方"]
                if pd.isna(bname) or str(bname).strip() == "":
                    continue
                c_prev = bus_row["成本金额_prev"] / 10000
                c_latest = bus_row["成本金额_latest"] / 10000
                f_prev = bus_row["计费金额_prev"] / 10000
                f_latest = bus_row["计费金额_latest"] / 10000
                p_prev = bus_row["毛利_new_prev"] / 10000
                p_latest = bus_row["毛利_new_latest"] / 10000
                p_diff = bus_row["毛利_diff"] / 10000
                p_rate = (p_diff / p_prev * 100) if p_prev !=0 else np.inf
                report_content += "%-16s %-8.2f万元 %-8.2f万元 %-8.2f万元 %-8.2f万元 %+8.2f万元 %+7.2f%%\n" % (
                    bname, c_prev, c_latest, f_prev, f_latest, p_diff, p_rate
                 )

    # 核心结论列出所有波动月份
    extra_line = ""
    if len(big_diff_months) > 0:
        extra_line = "## 核心结论\n"
        extra_line += "1. 共识别到 **%d** 个毛利波动超过3万元的月份:\n" % len(big_diff_months)
        extra_line += "2. 波动月份列表:\n"
        for idx, row in big_diff_months.iterrows():
            sign = "+" if row["毛利_diff"] > 0 else ""
            extra_line += "   - **%s**: %s%.2f万元\n" % (row["月份"], sign, row["毛利_diff"] / 10000)
    
    report_content += """
---

%s
""" % extra_line

    # 写入文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print("报告已生成: %s" % output_path)
    return report_content

def send_to_wechat_webhook(webhook_url, markdown_content, report_path):
    """推送报告到企业微信webhook"""
    import requests
    headers = {"Content-Type": "application/json"}
    try:
        # 提取需要的内容，保留从头部到核心结论之间的所有内容
        lines = markdown_content.split('\n')
        filtered_lines = []
        # 跳过开头的空的整体汇总部分
        skip = True
        for line in lines:
            if "- 波动月份前后两日指标对比" in line:
                skip = False
            if not skip:
                filtered_lines.append(line)
        
        # 保留头部 + 所有波动月份的对比 + 所有波动月份的客户+业务方下钻 + 核心结论
        push_content = ""
        # 先添加头部信息
        header_lines = markdown_content.split('- 波动月份前后两日指标对比')[0]
        push_content += header_lines
        push_content += "- 波动月份前后两日指标对比\n"
        
        # 添加过滤后的内容直到核心结论
        for line in filtered_lines:
            if "## 核心结论" in line:
                break
            push_content += line + "\n"
        
        # 添加核心结论
        if "## 核心结论" in markdown_content:
            conclusion_part = markdown_content.split("## 核心结论")[-1].strip()
            push_content += "\n---\n\n## 核心结论\n" + conclusion_part
        
        push_content += "\n\n> 【附件】完整报告路径: {report_path}".format(report_path=report_path)
        
        # 保证不超过企业微信4000字节限制
        push_content = push_content[:4000]
        
        # 推送markdown内容
        payload = {"msgtype": "markdown", "markdown": {"content": push_content}}
        response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        res_data = response.json()
        if res_data.get("errcode") == 0:
            print("✅ 报告已成功推送至企业微信")
            return True
        else:
            print("❌ 推送失败: %s" % res_data.get('errmsg', '未知错误'))
            return False
    except Exception as e:
        print("❌ 推送异常: %s" % str(e))
        return False

if __name__ == "__main__":
    # 配置
    OUTPUT_DIR = "/home/yanzhuxin/guany/reports/"
    OUTPUT_REPORT_PATH = OUTPUT_DIR + "月维度前后两日波动分析报告_排除七牛CDN.md"
    # 企业微信推送配置：使用测试版
    ENABLE_WECHAT_PUSH = True
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595"
    USE_TEST_WEBHOOK = True  # 使用测试webhook

    # 1. 获取数据
    print("===== 开始获取数据 =====")
    # 动态确定需要包含的月份范围：
    # 如果今天 >= 3号，筛选范围为【三个月前 ~ 当月】
    # 如果今天 < 3号，筛选范围为【四个月前 ~ 上个月】
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    today_date = today.date()
    yesterday_date = yesterday.date()
    
    if today.day >= 3:
        # 当前 >= 3号：上个月截止，往前推12个月
        # 计算上个月
        end_datetime = today.replace(day=1) - timedelta(days=1)
        end_month = end_datetime.strftime("%Y-%m")
        # 计算11个月前，总共12个月
        start_datetime = end_datetime.replace(day=1) - timedelta(days=11*30)
        start_month = start_datetime.strftime("%Y-%m")
    else:
        # 当前 < 3号：上上个月截止，往前推12个月
        # 计算上上个月
        end_datetime = today.replace(day=1) - timedelta(days=1)  # 上个月第一天
        end_datetime = end_datetime.replace(day=1) - timedelta(days=1)  # 上上个月最后一天
        end_month = end_datetime.strftime("%Y-%m")
        # 计算11个月前，总共12个月
        start_datetime = end_datetime.replace(day=1) - timedelta(days=11*30)
        start_month = start_datetime.strftime("%Y-%m")
    
    print("筛选月份范围: %s ~ %s (共12个月)" % (start_month, end_month))
    print("对比两天: %s (昨日) vs %s (今日)" % (yesterday_date, today_date))
    
    # 查询所有符合月份范围并排除七牛CDN，分别查询两天
    token = get_token()
    col = None
    all_preview = []
    colnames = []
    
    # 生成月份列表
    months = []
    current = datetime.strptime(start_month, "%Y-%m")
    end_datetime = datetime.strptime(end_month, "%Y-%m")
    while current <= end_datetime:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    # 只取最近3个月
    months = months[-3:]
    
    # 分别查询昨天和今天
    for query_date in [yesterday_date, today_date]:
        print("正在获取 %s 数据..." % query_date)
        for month in months:
            fc = FilterCondition()
            # 日期等于查询日期，排除七牛CDN，合并月份等于当前月份
            fc.eq("统计日期", query_date.strftime("%Y-%m-%d")).ne("客户_new", "七牛CDN").eq("合并月份", month)
            result = GuanDataFetcher.fetch_data(
                token=token, ds_id="eff95e2a2fe0048dfb9727b1", filter_condition=fc, limit=20000
            )
            if not colnames:
                col = result.get("columns", [])
                colnames = [col[i]["name"] for i in range(len(col))]
            preview = result.get("preview", [])
            all_preview.extend(preview)
            print("  - %s: got %d rows" % (month, len(preview)))
    
    df2 = pd.DataFrame(all_preview, columns=colnames)
    print("全部获取完成，共 %d 行" % len(df2))

    # 2. 数据预处理
    print("\n===== 开始数据预处理 =====")
    df = load_and_preprocess_data(df2)
    print("预处理完成")

    # 3. 获取对比日期
    latest_day = today_date
    prev_day = yesterday_date
    print("分析日期: %s (昨日) → %s (今日)" % (prev_day, latest_day))

    # 4. 计算月度指标对比
    print("\n===== 开始计算月度指标对比 =====")
    monthly_result = calculate_monthly_metrics(df, latest_day, prev_day)
    print("计算完成，共 %d 个月份符合条件" % len(monthly_result))

    # 5. 生成分析报告
    print("\n===== 生成分析报告 =====")
    report = generate_markdown_report(df, monthly_result, latest_day, prev_day, OUTPUT_REPORT_PATH)

    # 6. 推送至企业微信机器人（测试版）
    if ENABLE_WECHAT_PUSH:
        print("\n正在推送报告到企业微信测试版...")
        send_to_wechat_webhook(WECHAT_WEBHOOK, report, OUTPUT_REPORT_PATH)

    # 计算波动月份
    big_diff_months = monthly_result[abs(monthly_result["毛利_diff"]) > 30000].copy()
    
    # 打印核心结论
    print("\n===== 核心结论 =====")
    total_prev_profit = monthly_result["毛利_new_prev"].sum()
    total_latest_profit = monthly_result["毛利_new_latest"].sum()
    total_profit_diff = total_latest_profit - total_prev_profit
    total_profit_rate = (total_profit_diff / total_prev_profit * 100) if total_prev_profit !=0 else np.inf
    print(
        "总毛利变化: %+.2f (%+.2f%%)" % (total_profit_diff, total_profit_rate)
    )
    # 打印所有波动超过3万的月份
    if len(big_diff_months) > 0:
        print("\n存在毛利波动超过3万的月份:")
        for _, row in big_diff_months.iterrows():
            sign = "+" if row["毛利_diff"] > 0 else ""
            print("  - %s: %s%.2f" % (row["月份"], sign, row["毛利_diff"] / 10000))
