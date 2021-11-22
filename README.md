# DataX

Forked from https://github.com/alibaba/DataX

改动点：
* 升级了mongoReader 和 mongoWriter的jdbc依赖包
* 优化了mongoReader在大批量同步时的速度
* 增加了mongoReader读取时间戳类型的自动格式化转换操作，详见相关文档