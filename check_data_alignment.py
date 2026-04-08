#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import markdown
import os

sys.path.insert(0, "/Volumes/system/pypro")
from guandata_client import GuanDataFetcher, FilterCondition, get_token

# 全局配置
CONFIG = {
    "datasets": [
        {
            "id": "n8cace7a07ee7469dbcb7932",
            "name": "单机毛利表",
            "month_col": "月份",
            "metrics": {
                "amount": "整月计量金额",
                "cost": "整月成本金额",
                "bandwidth": "整月计量带宽",
                "cost_bandwidth": "整月成本带宽",
                "customer_col": "客户",
            },
        },
        {
            "id": "m8f91e0f7b25a46dba8f3666",
            "name": "观远账单数据",
            "month_col": "合并月份",
            "metrics": {
                "amount": "原始计费金额",
                "cost": "成本金额",
                "bandwidth": "本月计费带宽",
                "cost_bandwidth": "成本带宽G",
                "customer_col": "客户_new",
            },
        },
    ],
      "output": {
          "report": "/home/yanzhuxin/guany/reports/数据对齐校验报告.md",
          "customer_diff": "/home/yanzhuxin/guany/reports/customer_bandwidth_diff.csv",
          "alignment_detail": "/home/yanzhuxin/guany/reports/alignment_check_result.csv",
      },
      "threshold": 0.1,  # 允许差异率阈值(%)
      "webhook": {
          "enable_push": True,  # 开启企业微信推送
          "url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0d04ba7b-40e4-4502-bcce-bcbc60a2bfd4",
          "test_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595",
          "use_test": False,  # 使用正式webhook
      },
}


def fetch_datasets():
    """获取两个数据集（自动筛选指定月份的数据）
    动态逻辑：如果已经到了本月2号及以后，则筛选本月，否则筛选上月
    """
    from datetime import datetime
    today = datetime.today()
    # 动态计算筛选月份：本月2号及以后选本月，否则选上月
    if today.day >= 2:
        filter_month = today.strftime("%Y-%m")
    else:
        # 计算上月
        last_month = today.replace(day=1) - timedelta(days=1)
        filter_month = last_month.strftime("%Y-%m")
    print(f"动态筛选月份: 等于 {filter_month} (根据日期自动计算: 今天{today.day}号{'≥2号，选本月' if today.day >= 2 else '<2号，选上月'})")
    
    token = get_token()
    dfs = []
    for ds_config in CONFIG["datasets"]:
        print(f"正在获取{ds_config['name']} ID:{ds_config['id']}...")
        fc = FilterCondition()
        fc.eq(ds_config["month_col"], filter_month)
        result = GuanDataFetcher.fetch_data(
            token=token, ds_id=ds_config["id"], filter_condition=fc, limit=50000
        )
        columns = [col["name"] for col in result.get("columns", [])]
        df = pd.DataFrame(result.get("preview", []), columns=columns)
        # 数值字段转换为浮点型
        numeric_cols = [
            ds_config["metrics"]["amount"],
            ds_config["metrics"]["cost"],
            ds_config["metrics"]["bandwidth"],
            ds_config["metrics"]["cost_bandwidth"],
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        # 标准化月份字段
        df["month"] = pd.to_datetime(df[ds_config["month_col"]]).dt.strftime("%Y-%m")
        dfs.append(df)
        print(f"✅ {ds_config['name']}获取完成，共{len(df)}条记录")
    return dfs[0], dfs[1], filter_month


def calculate_alignment(df1, df2):
    """计算整体维度对齐情况"""
    ds1 = CONFIG["datasets"][0]
    ds2 = CONFIG["datasets"][1]

    # 按月份聚合
    agg1 = (
        df1.groupby("month")
        .agg(
            {
                ds1["metrics"]["amount"]: "sum",
                ds1["metrics"]["cost"]: "sum",
                ds1["metrics"]["bandwidth"]: "sum",
                ds1["metrics"]["cost_bandwidth"]: "sum",
            }
        )
        .reset_index()
    )

    agg2 = (
        df2.groupby("month")
        .agg(
            {
                ds2["metrics"]["amount"]: "sum",
                ds2["metrics"]["cost"]: "sum",
                ds2["metrics"]["bandwidth"]: "sum",
                ds2["metrics"]["cost_bandwidth"]: "sum",
            }
        )
        .reset_index()
    )

    merged = pd.merge(agg1, agg2, on="month", how="outer").fillna(0)

    # 计算校验结果
    result = {"monthly_details": [], "merged_data": merged, "conclusion": ""}
    all_pass = True

    for _, row in merged.iterrows():
        month = row["month"]
        # 各指标校验
        amount_diff = abs(row[ds1["metrics"]["amount"]] - row[ds2["metrics"]["amount"]])
        amount_rate = (
            amount_diff / row[ds2["metrics"]["amount"]] * 100
            if row[ds2["metrics"]["amount"]] != 0
            else 0
        )
        amount_pass = amount_rate <= CONFIG["threshold"]

        cost_diff = abs(row[ds1["metrics"]["cost"]] - row[ds2["metrics"]["cost"]])
        cost_rate = (
            cost_diff / row[ds2["metrics"]["cost"]] * 100
            if row[ds2["metrics"]["cost"]] != 0
            else 0
        )
        cost_pass = cost_rate <= CONFIG["threshold"]

        bw_diff = abs(
            row[ds1["metrics"]["bandwidth"]] - row[ds2["metrics"]["bandwidth"]]
        )
        bw_rate = (
            bw_diff / row[ds2["metrics"]["bandwidth"]] * 100
            if row[ds2["metrics"]["bandwidth"]] != 0
            else 0
        )
        bw_pass = bw_rate <= CONFIG["threshold"]

        cost_bw_diff = abs(
            row[ds1["metrics"]["cost_bandwidth"]]
            - row[ds2["metrics"]["cost_bandwidth"]]
        )
        cost_bw_rate = (
            cost_bw_diff / row[ds2["metrics"]["cost_bandwidth"]] * 100
            if row[ds2["metrics"]["cost_bandwidth"]] != 0
            else 0
        )
        cost_bw_pass = cost_bw_rate <= CONFIG["threshold"]

        month_pass = amount_pass and cost_pass and bw_pass and cost_bw_pass
        if not month_pass:
            all_pass = False

        result["monthly_details"].append(
            {
                "month": month,
                "amount_pass": amount_pass,
                "cost_pass": cost_pass,
                "bandwidth_pass": bw_pass,
                "cost_bw_pass": cost_bw_pass,
                "all_pass": month_pass,
                "amount": {
                    "ds1": row[ds1["metrics"]["amount"]],
                    "ds2": row[ds2["metrics"]["amount"]],
                    "diff": amount_diff,
                    "rate": amount_rate,
                },
                "cost": {
                    "ds1": row[ds1["metrics"]["cost"]],
                    "ds2": row[ds2["metrics"]["cost"]],
                    "diff": cost_diff,
                    "rate": cost_rate,
                },
                "bandwidth": {
                    "ds1": row[ds1["metrics"]["bandwidth"]],
                    "ds2": row[ds2["metrics"]["bandwidth"]],
                    "diff": row[ds1["metrics"]["bandwidth"]]
                    - row[ds2["metrics"]["bandwidth"]],
                    "rate": bw_rate,
                },
                "cost_bandwidth": {
                    "ds1": row[ds1["metrics"]["cost_bandwidth"]],
                    "ds2": row[ds2["metrics"]["cost_bandwidth"]],
                    "diff": cost_bw_diff,
                    "rate": cost_bw_rate,
                },
            }
        )

    result["conclusion"] = (
        "✅ 所有指标均对齐，数据完全一致"
        if all_pass
        else "⚠️ 存在部分指标未对齐，请查看详情"
    )
    merged.to_csv(CONFIG["output"]["alignment_detail"], encoding="utf-8", index=False)
    return result


def analyze_customer_diff(df1, df2):
    """客户维度下钻分析带宽差异"""
    ds1 = CONFIG["datasets"][0]
    ds2 = CONFIG["datasets"][1]

    # 按客户+月份聚合
    agg1 = (
        df1.groupby(["month", ds1["metrics"]["customer_col"]])
        .agg({ds1["metrics"]["bandwidth"]: "sum"})
        .reset_index()
    )

    agg2 = (
        df2.groupby(["month", ds2["metrics"]["customer_col"]])
        .agg({ds2["metrics"]["bandwidth"]: "sum"})
        .reset_index()
    )

    merged = pd.merge(
        agg1,
        agg2,
        left_on=["month", ds1["metrics"]["customer_col"]],
        right_on=["month", ds2["metrics"]["customer_col"]],
        how="outer",
    ).fillna(0)

    # 计算差异
    merged["diff"] = (
        merged[ds1["metrics"]["bandwidth"]] - merged[ds2["metrics"]["bandwidth"]]
    )
    merged["diff_abs"] = abs(merged["diff"])
    merged["diff_rate"] = merged.apply(
        lambda x: (
            x["diff_abs"] / x[ds2["metrics"]["bandwidth"]] * 100
            if x[ds2["metrics"]["bandwidth"]] != 0
            else np.inf
        ),
        axis=1,
    )

    merged = merged.sort_values(
        ["month", "diff_abs"], ascending=[True, False]
    ).reset_index(drop=True)
    merged.to_csv(CONFIG["output"]["customer_diff"], encoding="utf-8", index=False)
    return merged


def generate_markdown(alignment_result, customer_diff, filter_month):
    """生成适合企业微信展示的Markdown报告（无表格格式）"""
    ds1 = CONFIG["datasets"][0]
    ds2 = CONFIG["datasets"][1]

    report = f"""# 📊 数据集对齐校验报告
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
校验范围: {ds1["name"]} ↔ {ds2["name"]}
筛选条件: 月份 = {filter_month}
允许差异阈值: {CONFIG["threshold"]}%

---

## 一、整体校验结论
{alignment_result["conclusion"]}

---

## 二、分月份校验概览
"""
    for month_data in alignment_result["monthly_details"]:
        month = month_data["month"]
        status = "✅ 全部通过" if month_data["all_pass"] else "⚠️ 部分异常"
        report += f"### 📅 {month} {status}\n"
        report += (
            f"- 计量金额: {'✅ 对齐' if month_data['amount_pass'] else '❌ 异常'}\n"
        )
        report += f"- 成本金额: {'✅ 对齐' if month_data['cost_pass'] else '❌ 异常'}\n"
        report += (
            f"- 计量带宽: {'✅ 对齐' if month_data['bandwidth_pass'] else '❌ 异常'}\n"
        )
        report += (
            f"- 成本带宽: {'✅ 对齐' if month_data['cost_bw_pass'] else '❌ 异常'}\n\n"
        )

    report += """
---

## 三、已对齐指标明细
"""
    # 已对齐指标（金额、成本带宽）
    for month_data in alignment_result["monthly_details"]:
        month = month_data["month"]
        report += f"### {month}\n"
        # 计量金额
        amount_color = "green" if month_data["amount"]["rate"] < 0.1 else "red"
        report += f"**💴 整月计量金额 ↔ 原始计费金额**\n"
        report += f"> 数据集1: {month_data['amount']['ds1']:,.2f}\n"
        report += f"> 数据集2: {month_data['amount']['ds2']:,.2f}\n"
        report += f'> 差异: <font color="{amount_color}">{month_data["amount"]["diff"]:,.2f}</font> | 差异率: <font color="{amount_color}">{month_data["amount"]["rate"]:.6f}%</font>\n\n'
        # 成本金额
        cost_color = "green" if month_data["cost"]["rate"] < 0.1 else "red"
        report += f"**💸 整月成本金额 ↔ 成本金额**\n"
        report += f"> 数据集1: {month_data['cost']['ds1']:,.2f}\n"
        report += f"> 数据集2: {month_data['cost']['ds2']:,.2f}\n"
        report += f'> 差异: <font color="{cost_color}">{month_data["cost"]["diff"]:,.2f}</font> | 差异率: <font color="{cost_color}">{month_data["cost"]["rate"]:.6f}%</font>\n\n'
        # 成本带宽
        bw_color = "green" if month_data["cost_bandwidth"]["rate"] < 0.1 else "red"
        report += f"**📡 整月成本带宽 ↔ 成本带宽G**\n"
        report += f"> 数据集1: {month_data['cost_bandwidth']['ds1']:,.2f}G\n"
        report += f"> 数据集2: {month_data['cost_bandwidth']['ds2']:,.2f}G\n"
        report += f'> 差异: <font color="{bw_color}">{month_data["cost_bandwidth"]["diff"]:.4f}G</font> | 差异率: <font color="{bw_color}">{month_data["cost_bandwidth"]["rate"]:.6f}%</font>\n\n'

    report += """
---

## 四、异常指标明细
### ⚠️ 计量带宽差异
"""
    for month_data in alignment_result["monthly_details"]:
        month = month_data["month"]
        diff_color = "red" if abs(month_data["bandwidth"]["rate"]) > 1 else "orange"
        report += f"**{month}**\n"
        report += f"> 数据集1: {month_data['bandwidth']['ds1']:,.2f}G\n"
        report += f"> 数据集2: {month_data['bandwidth']['ds2']:,.2f}G\n"
        report += f'> 差异: <font color="{diff_color}">{month_data["bandwidth"]["diff"]:,.2f}G</font> | 差异率: <font color="{diff_color}">{month_data["bandwidth"]["rate"]:.2f}%</font>\n\n'

    report += """
---

## 五、客户维度差异Top10
"""
    months = sorted(customer_diff["month"].unique())
    for month in months:
        month_data = customer_diff[customer_diff["month"] == month].head(10)
        report += f"### 🏅 {month} 带宽差异Top10\n"
        for idx, row in month_data.reset_index(drop=True).iterrows():
            customer_name = (
                row[ds1["metrics"]["customer_col"]]
                if pd.notna(row[ds1["metrics"]["customer_col"]])
                else row[ds2["metrics"]["customer_col"]]
            )
            diff_color = "red" if abs(row["diff_rate"]) > 5 else "orange"
            report += f"{idx + 1}. **{customer_name}**\n"
            report += f"> 数据集1: {row[ds1['metrics']['bandwidth']]:,.2f}G | 数据集2: {row[ds2['metrics']['bandwidth']]:,.2f}G\n"
            report += f'> 差异: <font color="{diff_color}">{row["diff"]:,.2f}G</font> | 差异率: <font color="{diff_color}">{row["diff_rate"]:.2f}%</font>\n\n'

    report += f"""
---

## 六、文件说明
> 完整明细文件已生成：
> 1. 整体对齐明细：`{CONFIG["output"]["alignment_detail"]}`
> 2. 客户维度差异明细：`{CONFIG["output"]["customer_diff"]}`
"""

    with open(CONFIG["output"]["report"], "w", encoding="utf-8") as f:
        f.write(report)
    print(f"✅ Markdown报告已生成: {CONFIG['output']['report']}")

    # 同时生成HTML报告到下载目录供查看
    html_output_path = CONFIG["output"]["report"].replace(".md", ".html")
    html_content = markdown.markdown(
        report, extensions=["tables", "fenced_code", "nl2br"]
    )
    with open(html_output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"✅ HTML报告已生成: {html_output_path}")

    return report


def extract_core_conclusion(markdown_content):
    """提取核心结论部分"""
    lines = markdown_content.split('\n')
    core_conclusion = []
    in_core = False
    
    # 提取整体校验结论和分月份概览
    for line in lines:
        if "## 一、整体校验结论" in line:
            in_core = True
        elif "---" in line and in_core:
            core_conclusion.append(line)
            break
        elif in_core:
            core_conclusion.append(line)
    
    # 添加核心异常信息
    for line in lines:
        if "## 四、异常指标明细" in line:
            core_conclusion.append("\n")
            core_conclusion.append(line)
            break
    
    return '\n'.join(core_conclusion)

def send_to_wechat_webhook(markdown_content, alignment_result):
    """根据内容大小选择推送方式：小于4000字节直接推送，大于则推送结论+HTML文件"""
    if not CONFIG["webhook"]["enable_push"]:
        print("ℹ️  企业微信推送已关闭，如需开启请修改CONFIG中webhook.enable_push为True")
        return False

    # 选择使用正式还是测试webhook
    if CONFIG["webhook"].get("use_test", False):
        webhook_url = CONFIG["webhook"]["test_url"]
        print("ℹ️  使用测试webhook推送")
    else:
        webhook_url = CONFIG["webhook"]["url"]
        print("ℹ️  使用正式webhook推送")
    
    webhook_key = webhook_url.split("key=")[-1]
    content_size = len(markdown_content.encode('utf-8'))
    threshold = 4000  # 字节阈值
    
    try:
        if content_size <= threshold:
            # 小于等于4000字节，直接推送文本
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
            
            # 上传HTML文件
            temp_file_path = (
                f"/tmp/数据对齐校验报告_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
            )
            html_content = markdown.markdown(
                markdown_content, extensions=["tables", "fenced_code", "nl2br"]
            )
            with open(temp_file_path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={webhook_key}&type=file"
            files = {"media": open(temp_file_path, "rb")}
            upload_resp = requests.post(upload_url, files=files, timeout=10)
            upload_data = upload_resp.json()

            if upload_data.get("errcode") != 0:
                print(f"❌ 文件上传失败：{upload_data.get('errmsg', '未知错误')}")
                return False

            media_id = upload_data.get("media_id")

            # 发送文件消息
            payload = {"msgtype": "file", "file": {"media_id": media_id}}
            send_resp = requests.post(send_url, json=payload, headers=headers, timeout=10)
            send_data = send_resp.json()

            if send_data.get("errcode") == 0:
                print("✅ 核心结论和完整报告文件已成功推送至企业微信")
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
     print("=" * 80)
     print("开始执行数据集全流程对齐校验...")
     print("=" * 80)

     # 1. 获取数据集
     df1, df2, filter_month = fetch_datasets()

     # 2. 计算对齐情况
     print("\n正在计算整体对齐情况...")
     alignment_result = calculate_alignment(df1, df2)

     # 3. 客户维度下钻
     print("正在下钻客户维度差异...")
     customer_diff = analyze_customer_diff(df1, df2)

     # 4. 生成报告
     print("正在生成校验报告...")
     report_content = generate_markdown(alignment_result, customer_diff, filter_month)

     # 5. 推送企业微信（默认关闭）
     send_to_wechat_webhook(report_content, alignment_result)

     print("\n🎉 全流程执行完成！")

