#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
观远数据平台客户端
简洁的数据获取工具，仅使用Python标准库

使用示例:
    from guandata_client import FilterCondition, GuanDataFetcher
    
    # 方式1: 传参调用
    result = GuanDataFetcher.fetch_data(
        token="your_token",
        user_id="your_user_id",
        x_dom_id="your_x_dom_id",
        ds_id="your_ds_id",
        filter_condition=FilterCondition().eq("合并月份", "2026-03"),
        limit=50000
    )
    
    # 方式2: 使用环境变量默认值
    import os
    os.environ["GUANDATA_TOKEN"] = "your_token"
    os.environ["GUANDATA_USER_ID"] = "your_user_id"
    os.environ["GUANDATA_X_DOM_ID"] = "your_x_dom_id"
    os.environ["GUANDATA_DS_ID"] = "your_ds_id"
    
    result = GuanDataFetcher.fetch_data(
        filter_condition=FilterCondition().eq("合并月份", "2026-03")
    )
"""

import json
import os
import ssl
import urllib2
import urllib
import base64
import requests

# 创建SSL上下文（禁用证书验证，仅用于测试环境）
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# 默认配置（从环境变量读取，也可直接修改）
DEFAULT_CONFIG = {
    "token": os.getenv("GUANDATA_TOKEN", ""),
    "user_id": os.getenv("GUANDATA_USER_ID", ""),
    "x_dom_id": os.getenv("GUANDATA_X_DOM_ID", ""),
    "ds_id": os.getenv("GUANDATA_DS_ID", ""),
    "base_url": os.getenv("GUANDATA_BASE_URL", "https://d.qiniu.io")
}

def get_token():
    url = "https://d.qiniu.io/api/user/sign-in"
    raw_password = "XH%60n"
    encoded_password = base64.b64encode(raw_password.encode("utf-8")).decode("utf-8")
    payload = json.dumps(
        {
            "domain": "guanbi",
            "loginId": "yanzhuxin",
            "password": encoded_password,
        }
    )
    headers = {"Content-Type": "application/json"}
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()['uIdToken']
    return token


class FilterCondition:
    """
    观远数据筛选条件构造器
    
    示例:
        fc = FilterCondition()
        fc.eq("合并月份", "2026-03").gt("毛利_new", 0)
        filter_data = fc.build()
    """
    
    def __init__(self):
        self.conditions = []
    
    def _add(self, name, filter_type, value):
        """内部方法：添加条件"""
        if value is None:
            filter_value = []
        elif isinstance(value, list):
            filter_value = [str(v) for v in value]
        else:
            filter_value = [str(value)]
        
        self.conditions.append({
            "type": "condition",
            "value": {
                "name": name,
                "filterType": filter_type,
                "filterValue": filter_value
            }
        })
        return self
    
    def eq(self, name, value):
        """等于 EQ"""
        return self._add(name, "EQ", value)
    
    def gt(self, name, value):
        """大于 GT"""
        return self._add(name, "GT", value)
    
    def ge(self, name, value):
        """大于等于 GE"""
        return self._add(name, "GE", value)
    
    def lt(self, name, value):
        """小于 LT"""
        return self._add(name, "LT", value)
    
    def ne(self, name, value):
        """不等于 NE"""
        return self._add(name, "NE", value)
    
    def le(self, name, value):
        """小于等于 LE"""
        return self._add(name, "LE", value)
    
    def in_list(self, name, values):
        """在列表中 IN"""
        return self._add(name, "IN", values)
    
    def not_in(self, name, values):
        """不在列表中 NOT_IN"""
        return self._add(name, "NI", values)
    
    def like(self, name, pattern):
        """模糊匹配 LIKE"""
        return self._add(name, "LIKE", pattern)
    
    def is_null(self, name):
        """为空 IS_NULL"""
        return self._add(name, "IS_NULL", [])
    
    def is_not_null(self, name):
        """不为空 IS_NOT_NULL"""
        return self._add(name, "IS_NOT_NULL", [])
    
    def between(self, name, min_val, max_val):
        """在区间 BETWEEN"""
        return self._add(name, "BETWEEN", [min_val, max_val])
    
    def build(self, combine_type = "AND"):
        """构建筛选条件字典"""
        return {
            "combineType": combine_type,
            "conditions": self.conditions
        }
    
    def to_json(self, indent = 2):
        """转为JSON字符串"""
        return json.dumps(self.build(), ensure_ascii=False, indent=indent)
    
    def is_empty(self):
        """检查是否为空"""
        return len(self.conditions) == 0


class GuanDataFetcher:
    """
    观远数据获取客户端
    
    支持传参调用和使用默认配置
    """
    
    @staticmethod
    def fetch_data(
        token = DEFAULT_CONFIG["token"],
        ds_id = DEFAULT_CONFIG["ds_id"],
        base_url = DEFAULT_CONFIG["base_url"],
        filter_condition = None,
        offset = 0,
        limit = 50000
    ):
        """
        获取数据（单次请求）
        
        Args:
            token: 认证token（默认从环境变量GUANDATA_TOKEN读取）
            user_id: 用户ID（默认从环境变量GUANDATA_USER_ID读取）
            x_dom_id: DOM ID（默认从环境变量GUANDATA_X_DOM_ID读取）
            ds_id: 数据集ID（默认从环境变量GUANDATA_DS_ID读取）
            base_url: API基础地址（默认https://d.qiniu.io）
            filter_condition: 筛选条件对象
            offset: 起始位置
            limit: 返回条数
        
        Returns:
            API返回的JSON数据
        """
        url = "%s/api/data-source/%s/data" % (base_url, ds_id)
        
        # 构建请求体
        payload = {
            "offset": offset,
            "limit": limit
        }
        
        if filter_condition and not filter_condition.is_empty():
            payload["filter"] = filter_condition.build()
        
        # 构建headers
        headers = {
            "Content-Type": "application/json",
            "token": token
        }
        
        # 发送请求
        req = urllib2.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers
        )
        req.get_method = lambda: 'POST'
        
        try:
            response = urllib2.urlopen(req, timeout=30, context=SSL_CONTEXT)
            return json.loads(response.read().decode('utf-8'))
        except urllib2.HTTPError as e:
            return {"error": "HTTPError", "code": e.code, "message": str(e)}
        except Exception as e:
            return {"error": "Exception", "message": str(e)}
    
    @staticmethod
    def fetch_all(
        token = DEFAULT_CONFIG["token"],
        ds_id = DEFAULT_CONFIG["ds_id"],
        base_url = DEFAULT_CONFIG["base_url"],
        filter_condition = None,
        batch_size = 50000
    ):
        """
        获取所有数据（自动分页）
        
        Args:
            token: 认证token（默认从环境变量GUANDATA_TOKEN读取）
            user_id: 用户ID（默认从环境变量GUANDATA_USER_ID读取）
            x_dom_id: DOM ID（默认从环境变量GUANDATA_X_DOM_ID读取）
            ds_id: 数据集ID（默认从环境变量GUANDATA_DS_ID读取）
            base_url: API基础地址（默认https://d.qiniu.io）
            filter_condition: 筛选条件对象
            batch_size: 每批次数量
        
        Returns:
            合并后的数据字典
        """
        all_preview = []
        offset = 0
        total = None
        columns = []
        
        while True:
            result = GuanDataFetcher.fetch_data(
                token=token,
                ds_id=ds_id,
                base_url=base_url,
                filter_condition=filter_condition,
                offset=offset,
                limit=batch_size
            )
            
            # 检查错误
            if "error" in result:
                return result
            
            # 获取数据
            preview = result.get("preview", [])
            all_preview.extend(preview)
            
            # 保存列信息
            if not columns and "columns" in result:
                columns = result["columns"]
            
            # 获取总数
            if total is None:
                total = result.get("rowCount", 0)
            
            # 检查是否获取完成
            if len(preview) < batch_size or len(all_preview) >= total:
                break
            
            offset += batch_size
        
        return {
            "dsId": ds_id,
            "rowCount": len(all_preview),
            "total": total,
            "preview": all_preview,
            "columns": columns
        }


# ==================== 使用示例 ====================

if __name__ == "__main__":
    # 示例配置（可通过环境变量设置，或直接传参）
    EXAMPLE_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxLXRoV0FJRS9oKzZBUE1KeGNLTzhLaWdnd0VMOE9CRkdaanVBMGROMmJma0V1a3pVL0ZEZVc2N2tMdGtqUjQ5SFk4ajZWb3lWWVl2S2p3aFluQUpkWGpKRUtBQzM5Vkptdkd5YWFnalE3b3JlQVhhdUZrbkR1T090WUtnPT0iLCJhdXRvTG9nb3V0T25DbG9zZUVuYWJsZWQiOmZhbHNlLCJpc3MiOiJndWFuZGF0YS5jb20iLCJleHAiOjE3NzQyMjc5MDMsImlhdCI6MTc3MzM2Njc0MiwiaW5pdFRpbWUiOiIyMDI2LTAzLTA5IDA5OjA1OjAzLjc1MiIsImp0aSI6IjYxMzUwMTY3MTRlYzkxOTQ5NzQzNWQwYWUzZmJmMzM3M2EzNGM2MWRmYmYwN2NiNjljNTJlNzc2MjY4ZGM4NjcyMDUxZTY4OWFmNmE5ZDE0MWExYjQxNDE0NTcyMGQ5MzYxMzM0NjY5ZWNjYmZhNGRhNDc0NjNhODkyNWJkY2YxNWJiYWIyN2NmMmFkMjUyYjJhNDIwM2YyN2ExZGQwZTdiN2JkNWZhZDUwZDIzZGYzYjMyODg4MWZlYjIyZTVlMTc4NDM1NzRhNTcyY2Y1MDk5MDYwZTE1YTdkZjUxYTEwYWM5ZDIxMjFmZDA5NzllODVlODdhNTFlZjJkNzhhNWQifQ.4FqTX0ZA5BWG4ui2_o2p5Q6upiFIPqV5gd9nq4rfCHM"
    EXAMPLE_USER_ID = "eWFuemh1eGlu"
    EXAMPLE_X_DOM_ID = "Z3VhbmJp"
    EXAMPLE_DS_ID = "eff95e2a2fe0048dfb9727b1"
    
    print("=" * 80)
    print("观远数据客户端 - 使用示例")
    print("=" * 80)
    
    # 示例1: 直接传参（推荐）
    print("\n【示例1】直接传参调用")
    print("-" * 80)
    
    result1 = GuanDataFetcher.fetch_data(
        token=EXAMPLE_TOKEN,
        ds_id=EXAMPLE_DS_ID,
        limit=10
    )
    print("返回条数: %d" % len(result1.get('preview', [])))
    
    # 示例2: 传参 + 筛选条件
    print("\n【示例2】传参 + 筛选条件")
    print("-" * 80)
    
    fc = FilterCondition()
    fc.eq("合并月份", "2026-03")
    
    result2 = GuanDataFetcher.fetch_data(
        token=EXAMPLE_TOKEN,
        ds_id=EXAMPLE_DS_ID,
        filter_condition=fc,
        limit=50000
    )
    print("返回条数: %d" % len(result2.get('preview', [])))
    
    # 验证筛选结果
    preview = result2.get("preview", [])
    if preview:
        months = set([row[15] for row in preview if len(row) > 15])
        print("合并月份分布: %s" % months)
    
    # 示例3: 链式构造筛选条件
    print("\n【示例3】链式构造筛选条件")
    print("-" * 80)
    
    result3 = GuanDataFetcher.fetch_data(
        token=EXAMPLE_TOKEN,
        ds_id=EXAMPLE_DS_ID,
        filter_condition=FilterCondition().eq("合并月份", "2026-03").gt("毛利_new", 0),
        limit=10
    )
    print("返回条数: %d" % len(result3.get('preview', [])))
    
    # 示例4: 自动分页获取全部数据
    print("\n【示例4】自动分页获取全部数据（只取100条示例）")
    print("-" * 80)
    
    result4 = GuanDataFetcher.fetch_all(
        token=EXAMPLE_TOKEN,
        ds_id=EXAMPLE_DS_ID,
        filter_condition=FilterCondition().eq("合并月份", "2026-03"),
        batch_size=50000
    )
    print("返回条数: %d" % result4.get('rowCount', 0))
    
    print("\n" + "=" * 80)
    print("示例结束")
    print("=" * 80)
