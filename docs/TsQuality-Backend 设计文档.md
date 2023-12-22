# TsQuality-Backend 设计文档

## 1. 项目概述

本项目是项目 TsQuality 的后端系统，旨在为前端系统提供数据支持。



## 2. 架构概览

### 2.1 技术栈

- 后端框架：[SpringBoot](https://spring.io/projects/spring-boot/) 3.1.4
- 数据库：[Mariadb](https://mariadb.org/) 3.3.0
- 持久化层框架：[MyBatis](https://mybatis.org/mybatis-3/) 3.3.0

### 2.2 系统架构图



## 3. 模块划分



## 4. 代码组织

### 4.1 common

全局工具函数，如：

- DataQualityCalculationUtil：根据原始时间序列数据的统计信息计算四个数据质量指标的值
- IoTDBUtil：与 IoTDB Server 进行通信时的通用操作，如构造 IoTDB 查询语句、根据时间序列路径查询数据

### 4.2 config

全局配置，如跨域配置等

### 4.3 controller

控制器层，包括：

#### 4.3.1 dataprofile

与 IoTDB 数据画像相关的控制器，包括：

- DataProfileController：与获取 IoTDB 数据画像相关的各个接口，如获取 IoTDB 中总数据点个数、总数据质量概览

#### 4.3.2 timeseries

与 IoTDB 中时间序列相关的控制器，包括：

- TimeSeriesOverviewController：与获取时间序列数据的概览信息相关的各个接口，如数据点总数、数据质量总览信息
- TimeSeriesDataController：与获取时间序列原始数据相关的各个接口，如获取最新 N 个点的数据、根据查询条件查询原始数据
- TimeSeriesAnomalyDetectionController：与时间序列异常检测与修复相关的各个接口，如计算给定时间序列的所有异常点并给出相应修复值
- TimeSeriesDataQualityController：与时间序列数据质量分析相关的各个接口，如按时间维度查询时间序列的数据质量情况

#### 4.3.3 device

与 IoTDB 中设备相关的控制器，包括：

- DeviceOverviewController：与获取设备概览信息相关的各个接口，如获取设备总数、数据质量总览信息

#### 4.3.4 database

与 IoTDB 中数据库相关的控制器，包括：

- DatabaseOverviewController：与获取数据库概览信息相关的各个接口，如获取数据库总数、数据质量总览信息

### 4.4 exceptions

各种异常类型的定义

### 4.5 mappers

数据持久化层，负责数据库数据与Java内存对象数据的相互转换

### 4.6 models

#### 4.6.1 dtos

DTO(Data Transfer Object)，与前端通信的各数据格式的定义

#### 4.6.2 datastructures

系统业务实现逻辑中用到的各种辅助数据结构，包括：

- TimeRange：包含了起止时间戳的一个时间范围，可用于 IoTDB 数据查询等

### 4.7 services

与上述 controllers 包中各控制器一一对应的服务层实现



## 5. 数据库设计

### 5.1 表设计

#### 5.1.1 时间序列表 (series)



#### 5.1.2 TsFile表 (files)



#### 5.1.3 TsFile chunk表（chunks）



#### 5.1. 4 TsFile page表（pages）



#### 5.1.5 TsFile统计信息表（file_series_stats)



#### 5.1.6 chunk统计信息表（chunk_series_stats）



#### 5.1.7 page统计信息表（page_series_stats）

