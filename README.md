# DataX

Forked from https://github.com/alibaba/DataX

改动点：
* 升级了mongoReader 和 mongoWriter的jdbc依赖包
* 优化了mongoReader在大批量同步时的速度
* 增加了mongoReader读取时间戳类型的自动格式化转换操作，详见相关文档

2022-09-06
* 修改了mongoReader，支持字符串时间戳在读取时自动转换为Date类型，支持JSON类型读取Array字段
* 增加了StarRocks Writer。支持使用json load形式导入数据到表中的json字段（需要StarRocks版本在2.2以上）
