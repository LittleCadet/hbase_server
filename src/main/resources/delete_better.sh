#!/bin/bash

# 该脚本作用：删除根据timestamp删除hbase上指定表的列数据，运行后需要手动执行flush + major_compact的操作
# 该脚本与delete.sh的区别： 该脚本将要执行的语句写入文件，之后'hbase shell 文件名'的方式，一次连接，一次释放，快速处理数据。
# 而delete.sh的缺点是： 每次循环都会，建立一次hbase连接 + 处理数据+ 连接释放  ： 一次流程大概4s中
 dealCount=0
 table=app
 column=base
 start=1262275200000
 end=1611279855781
 hbase_home=${HBASE_HOME}/bin/hbase
# scan出指定时间段内的数据
 echo "scan '${table}',{COLUMNS=>'${column}', TIMERANGE=>[${start},${end}]}" | ${hbase_home} shell | grep 'column'| grep 'timestamp'  |awk '{print $1 $2}'| sed 's/column=/_/g' | sed 's/,//g' > rowkey.txt
 total=`wc -l rowkey.txt | awk '{print $1}'`
 echo "hbase检索${table}完毕，待处理数据量:"$total
 # 逐行读取文件
 cat rowkey.txt | while read line
 do
   str=${line}
   # 用tr将字符串按指定规则转换为数组
   arr=(`echo $str | tr '_' ' '`)
   #echo 'rowkey:'${arr[0]}',column:'${arr[1]}
   # 调用delete删除hbase中的数据
   echo "delete '${table}', '${arr[0]}','${arr[1]}'"
   dealCount=`expr $dealCount + 1`
   #echo "已处理数据:"$dealCount
 done > process.txt
 echo "hbase处理表：${table}完成"