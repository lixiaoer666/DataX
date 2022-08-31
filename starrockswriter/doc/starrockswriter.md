### Datax StarRocks writer
#### 1 快速介绍

StarRocks Writer是通过DataX框架实现的StarRocks写入插件，用于在数据迁移时将数据高性能的写入StarRocks数据库。

#### 2 实现原理

StarRocks Writer通过Datax框架获取Reader生成的数据，根据配置转换为csv或者json格式，然后进行攒批处理，攒批完成后通过stream load方式写入StarRocks数据库，从而更性能的将数据迁移到StarRocks数据库。

#### 3 功能说明
* 该示例从MongoDB读一份数据到StarRocks。

		{
        "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "odpsreader",
                    "parameter": {
                        "accessId": "********",
                        "accessKey": "*********",
                        "project": "tb_ai_recommendation",
                        "table": "jianying_tag_datax_test",
                        "column": [
                            "unique_id",
                            "sid",
                            "user_id",
                            "auction_id",
                            "content_type",
                            "pool_type",
                            "frontcat_id",
                            "categoryid",
                            "gmt_create",
                            "taglist",
                            "property",
                            "scorea",
                            "scoreb"
                        ],
                        "splitMode": "record",
                        "odpsServer": "http://xxx/api"
                    }
                },
                "writer": {
                    "name": "mongodbwriter",
                    "parameter": {
                        "address": [
                            "127.0.0.1:27017"
                        ],
                        "userName": "",
                        "userPassword": "",
                        "dbName": "tag_per_data",
                        "collectionName": "tag_data",
                        "column": [
                            {
                                "name": "unique_id",
                                "type": "string"
                            },
                            {
                                "name": "sid",
                                "type": "string"
                            },
                            {
                                "name": "user_id",
                                "type": "string"
                            },
                            {
                                "name": "auction_id",
                                "type": "string"
                            },
                            {
                                "name": "content_type",
                                "type": "string"
                            },
                            {
                                "name": "pool_type",
                                "type": "string"
                            },
                            {
                                "name": "frontcat_id",
                                "type": "Array",
                                "splitter": " "
                            },
                            {
                                "name": "categoryid",
                                "type": "Array",
                                "splitter": " "
                            },
                            {
                                "name": "gmt_create",
                                "type": "string"
                            },
                            {
                                "name": "taglist",
                                "type": "Array",
                                "splitter": " "
                            },
                            {
                                "name": "property",
                                "type": "string"
                            },
                            {
                                "name": "scorea",
                                "type": "int"
                            },
                            {
                                "name": "scoreb",
                                "type": "int"
                            },
                            {
                                "name": "scorec",
                                "type": "int"
                            }
                        ],
                        "writeMode": {
                        	"isReplace": "true",
                        	"replaceKey": "_id"
                        }
                    }
                }
            }
        ]
		}
		}

#### 4 参数说明

* address： StarRocks的数据地址信息，因为StarRocks可能是个集群，需要以数组的形式给出。【必填】
* userName：用户名。【必填】
* userPassword： 密码。【必填】
* database： 库名。【必填】
* table： 表名。【必填】
* columns：列名逗号分隔。【必填】
* format：json或者csv格式导入，默认csv。【选填】
* row_delimiter：行分隔符，默认\n。 【选填】
* label_prefix：label前缀，默认datax_。【选填】

#### 5 类型转换



#### 6 性能报告
#### 7 测试报告
