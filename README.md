
# hologres_text2data  

**Author:** hologres_dev  
**Version:** 0.1.0  
**Type:** Tool  

## Description  
A tool that converts natural language into secure and optimized SQL queries supporting Hologres syntax.
This plugin has been launched on the official Dify marketplace. You can directly use the [hologres_text2data](https://marketplace.dify.ai/plugins/hologres_dev/hologres_text2data) on the official marketplace.

## Quick Start  
### SQL Generation Component  
1. Introduce the `hologres_text2data` plugin.  
2. Complete basic parameter configuration.  

| Parameter Name | Type       | Required | Description                        |  
|----------------|------------|----------|------------------------------------|  
| db_type        | Select     | Yes      | Database type (hologres)           |  
| host           | String     | Yes      | Database host address              |  
| port           | Number     | Yes      | Database port (1-65535)            |  
| db_name        | String     | Yes      | Target database name               |  
| table_name     | String     | No       | Multiple tables separated by commas (empty for all databases) |  

3. Select a model  
The `Qwen2.5-72b-instruct` model is recommended. Other models can be tried independently. DeepSeek models are not supported.  

4. Use natural language to generate SQL query statements.  

### SQL Execution Component  
1. Introduce the `hologres_excute_sql` plugin.  
2. Complete basic parameter configuration.  

| Parameter Name | Type       | Required | Description                        |  
|----------------|------------|----------|------------------------------------|  
| db_type        | Select     | Yes      | Database type (hologres)           |  
| host           | String     | Yes      | Database host address              |  
| port           | Number     | Yes      | Database port (1-65535)            |  
| db_name        | String     | Yes      | Target database name               |  
| sql            | String     | Yes      | SQL query statement                |  

3. Click "Execute" to run the SQL statement.

# NOTICE
This plugin is developed by Alibaba Cloud and based on [https://github.com/jaguarliuu/rookie_text2data](https://github.com/jaguarliuu/rookie_text2data)  program.
Code is distributed under the Apache License (Version 2.0).