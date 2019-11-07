1.颜色说明

green 正常

yellow  副本不正常

red  部分正常

使用api查看状态:
http://192.168.10.102:9200/_cat/health?v
```
epoch      timestamp cluster        status node.total node.data shards pri relo init unassign pending_tasks max_task_wait_time active_shards_percent
1556551165 15:19:25  docker-cluster green           1         1      0   0    0    0        0             0                  -                100.0%
```
查看集群：http://192.168.10.102:9200/_cluster/health?pretty
```json
{
    "cluster_name": "docker-cluster",
    "status": "green",
    "timed_out": false,
    "number_of_nodes": 1,
    "number_of_data_nodes": 1,
    "active_primary_shards": 0,
    "active_shards": 0,
    "relocating_shards": 0,
    "initializing_shards": 0,
    "unassigned_shards": 0,
    "delayed_unassigned_shards": 0,
    "number_of_pending_tasks": 0,
    "number_of_in_flight_fetch": 0,
    "task_max_waiting_in_queue_millis": 0,
    "active_shards_percent_as_number": 100
}
```
查看节点的状态：http://192.168.10.102:9200/_cat/nodes?v
```
ip         heap.percent ram.percent cpu load_1m load_5m load_15m node.role master name
172.17.0.2           17          96   1    0.12    0.07     0.08 mdi       *      iv3rENL
```
查看所有索引:http://192.168.10.102:9200/_cat/indices?v

es后台启动使用 ./elasticsearch -d

Cluster:  根据集群名字，将机器添加到集群中，一个集群包含多个节点，名称默认为elasticsearch

Node: 节点的名称为随机取的

Index: 一堆相似结构的文档。

Type: 一个Index可以有多个Type，一个Type有多个Docuemnt

Document：一条记录

Field：字段。

MySQL      ES

DataBase    Index

Table       type

row         Document

column      field

-X http请求的方式 HEAD/GET/POST/PUT/DELETE
-d 传递数据

创建索引库

curl -XPUT 'http://192.168.10.102:9200/xiaoyao'
{"acknowledged":true,"shards_acknowledged":true,"index":"xiaoyao"}


索引的注意事项:
```
Index name limitations
There are several limitations to what you can name your index. The complete list of limitations are:

Lowercase only
Cannot include \, /, *, ?, ", <, >, |, ` ` (space character), ,, #
Indices prior to 7.0 could contain a colon (:), but that’s been deprecated and won’t be supported in 7.0+
Cannot start with -, _, +
Cannot be . or ..
Cannot be longer than 255 bytes (note it is bytes, so multi-byte characters will count towards the 255 limit faster)
```
创建数据：
```
curl -XPOST -H 'content-type:application/json' 'http://192.168.10.102:9200/xiaoyao/student/1' -d '{"name":"yishengxiaoyao","age":20,"interest":["spark","es"]}'
{"_index":"xiaoyao","_type":"student","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
```
查找数据
```
curl -XGET 'http://192.168.10.102:9200/xiaoyao/student/1'
```
```json
{
    "_index": "xiaoyao",
    "_type": "student",
    "_id": "1",
    "_version": 1,
    "_seq_no": 0,
    "_primary_term": 1,
    "found": true,
    "_source": {
        "name": "yishengxiaoyao",
        "age": 20,
        "interest": [
        "spark",
        "es"
        ]
    }
}
```
PUT与POST数据：POST 新增(新增一条记录，会有多条记录) PUT修改(如果有，就修改，如果没有，新增，只有一条记录)

```
curl -XPOST -H 'content-type:application/json' 'http://192.168.10.102:9200/xiaoyao/student/' -d '{"name":"xiaoyaoyisheng","age":20,"interest":["spark","es"]}'
{"_index":"xiaoyao","_type":"student","_id":"HfHJaWoBUyPOGqhh-a9t","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
```
上面这个是自动生成的id。

DocId 可以手工指定，也可以系统生成，不要冲突就行。

GUID算法



使用页面方式查(粒度要细):
```
http://192.168.10.102:9200/_search?pretty
http://192.168.10.102:9200/xiaoyao/student/_search?pretty
http://192.168.10.102:9200/xiaoyao/student/1
```

curl方式
```
curl -XGET 'http://192.168.10.102:9200/xiaoyao/student/1?_source_include=name&pretty' 
{
  "_index" : "xiaoyao",
  "_type" : "student",
  "_id" : "1",
  "_version" : 1,
  "_seq_no" : 0,
  "_primary_term" : 1,
  "found" : true,
  "_source" : {
    "name" : "yishengxiaoyao"
  }
}

curl -XGET 'http://192.168.10.102:9200/xiaoyao/student/1?_source=name&pretty' 
{
  "_index" : "xiaoyao",
  "_type" : "student",
  "_id" : "1",
  "_version" : 1,
  "_seq_no" : 0,
  "_primary_term" : 1,
  "found" : true,
  "_source" : {
    "name" : "yishengxiaoyao"
  }
}


curl -XGET 'http://192.168.10.102:9200/xiaoyao/student/1?_source_exclude=name&pretty' 
{
  "_index" : "xiaoyao",
  "_type" : "student",
  "_id" : "1",
  "_version" : 1,
  "_seq_no" : 0,
  "_primary_term" : 1,
  "found" : true,
  "_source" : {
    "interest" : [
      "spark",
      "es"
    ],
    "age" : 20
  }
}

curl -XGET 'http://192.168.10.102:9200/xiaoyao/student/1/_source' 
{"name":"yishengxiaoyao","age":20,"interest":["spark","es"]}

```

```
curl -XGET 'http://192.168.10.103:9200/xiaoyao/student/1?pretty' 
{
  "_index" : "xiaoyao",
  "_type" : "student",
  "_id" : "1",
  "_version" : 1,
  "_seq_no" : 0,
  "_primary_term" : 1,
  "found" : true,
  "_source" : {
    "name" : "yishengxiaoyao",
    "age" : 20,
    "interest" : [
      "spark",
      "es"
    ]
  }
}
```
```
curl -XGET 'http://192.168.10.103:9200/xiaoyao/student/_search?pretty' -H 'Content-Type:application/json' -d '{"query":{"match":{"name":"xiaoyao"}}}' 
{
  "took" : 94,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 0,
    "max_score" : null,
    "hits" : [ ]
  }
}
```
```
 curl -XGET 'http://192.168.10.103:9200/_mget?pretty' -H 'Content-Type:application/json' -d '{"docs":[{"_index":"xiaoyao", "_type":"student","_id":1},{"_index":"xiaoyao", "_type":"student","_id":100}]}' 
{
  "docs" : [
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "1",
      "_version" : 1,
      "_seq_no" : 0,
      "_primary_term" : 1,
      "found" : true,
      "_source" : {
        "name" : "yishengxiaoyao",
        "age" : 20,
        "interest" : [
          "spark",
          "es"
        ]
      }
    },
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "100",
      "found" : false
    }
  ]
}
```
```
 curl -XGET 'http://192.168.10.103:9200/xiaoyao/student/_mget?pretty' -H 'Content-Type:application/json' -d '{"docs":[{"_id":1},{"_id":100}]}' 
{
  "docs" : [
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "1",
      "_version" : 1,
      "_seq_no" : 0,
      "_primary_term" : 1,
      "found" : true,
      "_source" : {
        "name" : "yishengxiaoyao",
        "age" : 20,
        "interest" : [
          "spark",
          "es"
        ]
      }
    },
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "100",
      "found" : false
    }
  ]
}
```
```
curl -XGET 'http://192.168.10.103:9200/xiaoyao/student/_mget?pretty' -H 'Content-Type:application/json' -d '{"ids":["1","10000"]}'
{
  "docs" : [
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "1",
      "_version" : 1,
      "_seq_no" : 0,
      "_primary_term" : 1,
      "found" : true,
      "_source" : {
        "name" : "yishengxiaoyao",
        "age" : 20,
        "interest" : [
          "spark",
          "es"
        ]
      }
    },
    {
      "_index" : "xiaoyao",
      "_type" : "student",
      "_id" : "10000",
      "found" : false
    }
  ]
}
```

```
curl -XPOST -H 'Content-Type:application/json' 'http://192.168.10.103:9200/xiaoyao/student/998' -d '{"name":"xiaoyaosan","age":30,"interests":["football","it"] }'
{"_index":"xiaoyao","_type":"student","_id":"998","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":2}
```
全量更新:就是新的增加一条
```
curl -XPUT -H 'Content-Type:application/json' 'http://192.168.10.103:9200/xiaoyao/student/998' -d '{"name":"xiaoyaosan_update"}'
{"_index":"xiaoyao","_type":"student","_id":"999","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":2}
```
局部更新:在更新的时候，需要带更新符号
```
curl -XPOST -H 'Content-Type:application/json' 'http://192.168.10.103:9200/xiaoyao/student/998/_update' -d '{"doc":{ "name":"xiaoyaosan_update", "work":"BigData R&D" }}'
{"_index":"xiaoyao","_type":"student","_id":"998","_version":2,"result":"updated","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":2,"_primary_term":2}
```

```
curl -XPUT -H 'Content-Type:application/json' 'http://192.168.10.103:9200/xiaoyao/student/998' -d '{"name":"xiaoyaosan_update_1"}'
{"_index":"xiaoyao","_type":"student","_id":"998","_version":3,"result":"updated","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":3,"_primary_term":2}
```
删除
```
curl -XDELETE http://192.168.10.103:9200/xiaoyao/student/998 
{"_index":"xiaoyao","_type":"student","_id":"998","_version":4,"result":"deleted","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":4,"_primary_term":2}
```
Cluster:多个节点

shard:索引分片(默认为5个，可以通过number_of_shards来设置，已经创建，不能再次修改)

进行设置分片:
```
curl -XPUT 'http://192.168.10.103:9200/xiaoyaotest' -H 'Content-Type:application/json' -d'{"settings":{"number_of_shards":3}}'
{"acknowledged":true,"shards_acknowledged":true,"index":"xiaoyaotest"}
```
设置副本(默认为1，number_of_replicas):
```
curl -XPUT '192.168.10.103:9200/xiaoyaosan2' -H 'Content-Type:application/json' -d'{"settings":{"number_of_shards":3,"number_of_replicas":2}}'
{"acknowledged":true,"shards_acknowledged":true,"index":"xiaoyaosan2"}
```
Transport:Cluster内部或者集群与Client的交互方式。

###SEARCHTYPE

_score:匹配度的问题

1.数量问题：top10  返回50

2.排名问题:

QUERY_AND_FETCH:往每一个分片发送请求，将各自的数据返回，  优点：一次查询 缺点：数据N倍

QUERY_THEN_FETCH: 向所有shard发请求,只拿到每一个分片的DocID+rank(score,排名),根据score重新排名,取前N个Doc,根据DocId然后再去shard中获取数据
优点:数据量准确  缺点:数据不一定对，性能比第一种差，发两次请求   

DFS_QUERY_THEN_FETCH:在查询之前，先对所有分片发送请求，把所有分片中的score全部汇总到一个地方，再执行后续的也操作,
优点:数据量对，结果对     缺点:性能差。

DFS比没DFS的准确度高，但是性能差
```

```