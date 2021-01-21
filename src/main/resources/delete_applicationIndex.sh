#!/bin/bash

# 该脚本作用：删除根据timestamp删除hbase上指定表的列数据，运行后需要手动执行flush + major_compact的操作
# 该脚本局限性： 对于rowkey是byte数组的，无能为力： 原因： 脚本读取出来的rowkey是byte数组的字符串，所以运行脚本时，hbase会因为rowkey类型不匹配，导致删除不成功【备注： 脚本没有具体的报错信息。会一直运行】

 dealCount=0
 table=ApplicationIndex
 column=Agents
 start=1262275200000
 end=1609430400000
 hbase=/home/hadoop/hbase-1.2.6/bin/hbase
# scan出指定时间段内的数据
 echo "scan '${table}',{COLUMNS=>'${column}', TIMERANGE=>[${start},${end}]}" | ${hbase} shell | grep 'column'| grep 'timestamp'  |awk '{print $1 $2}'| sed 's/column=/|/g' | sed 's/,//g' >rowkey.txt
 total=`wc -l rowkey.txt | awk '{print $1}'`
 echo "hbase检索${table}完毕，待处理数据量:"$total
 # 逐行读取文件
 cat rowkey.txt | while read line
 do
   str=${line}
   # 用tr将字符串按指定规则转换为数组
   arr=(`echo $str | tr '|' ' '`)
   echo 'rowkey:'${arr[0]}',column:'${arr[1]}
   # 调用delete删除hbase中的数据
   echo "delete '${table}', '${arr[0]}','${arr[1]}'"| ${hbase} shell > result.txt
   dealCount=`expr $dealCount + 1`
   echo "已处理数据:"$dealCount
 done
 echo "hbase处理表：${table}完成"