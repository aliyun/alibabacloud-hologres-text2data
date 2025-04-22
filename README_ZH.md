# hologres_text2data

**Author:** hologres_dev
**Version:** 0.1.0
**Type:** tool

## Description
将自然语言转换为安全、优化的支持 Hologres 语法的 SQL 查询工具。
该插件已经登陆 Dify 官方市场，您可以直接在官方市场上使用[hologres_text2data](https://marketplace.dify.ai/plugins/hologres_dev/hologres_text2data)

## 快速开始
### SQL生成组件
1. 引入 hologres_text2data 插件
2. 完成基础参数配置

| 参数名    | 类型     | 必填 | 描述                           |
|-----------|----------|------|-----------------------------|
| db_type   | select   | 是   | 数据库类型（hologres）         | 
| host      | string   | 是   | 数据库主机地址                 | 
| port      | number   | 是   | 数据库端口（1-65535）          |
| db_name   | string   | 是   | 目标数据库名称                 |
| table_name| string   | 否   | 多表逗号分隔（空则全库）         |

3. 选择模型
推荐使用`Qwen2.5-72b-instruct`模型，其他模型请自行尝试。不支持深度思考模型。

4. 使用自然语言生成 SQL 查询语句

### SQL执行组件
1. 引入 hologres_excute_sql 插件
2. 完成基础参数配置

| 参数名    | 类型     | 必填 | 描述                           |
|-----------|----------|------|-----------------------------|
| db_type   | select   | 是   | 数据库类型（hologres）         |
| host      | string   | 是   | 数据库主机地址                 | 
| port      | number   | 是   | 数据库端口（1-65535）          | 
| db_name   | string   | 是   | 目标数据库名称                 |
| sql       | string   | 是   | SQL 查询语句                  |

3. 点击执行，执行 sql 语句

# 声明
本插件由阿里云开发，初版代码基于 [https://github.com/jaguarliuu/rookie_text2data](https://github.com/jaguarliuu/rookie_text2data) 项目。  
代码根据 Apache 许可证（版本 2.0）分发。