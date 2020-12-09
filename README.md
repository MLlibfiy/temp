# 1、大数据分析

# 1.1、数据准备

1.  将数据上传到Hdfs

```shell

# 创建文件目录
hadoop dfs -mkdir /user/ylyu001/data/in

# 执行文件上传命令
# 在200704hourly.txt文件所在的位置执行如下命令

hadoop dfs -put 200704hourly.txt /user/ylyu001/data/in/

# 查看是否上传成功
hadoop dfs -ls  /user/ylyu001/data/in/
# 如果成功会打印出文件名

```

## 1.2、练习1

- 编写mapper函数，查找给定气象站每天的最大(最小)“干球温度”

1. 上传jar包到服务器

将temp-1.0.jar 上传到服务器任意目录

2. 执行MapReduce程序

```shell

# 提交任务, 03131是气象站编号

hadoop jar temp-1.0.jar com.temp.QueryTempByStation 03131

# 等待任务完成

```

3. 查看结果

```shell

# 查看结果
# 03131 事气象站编号

hadoop dfs -cat /user/ylyu001/data/station_temp-03131/part-r-00000

# 结果如下
气象站,日期,最高温度,最低温度
03131,20070401,72,48
03131,20070402,66,54
03131,20070403,67,54
03131,20070404,70,54
03131,20070405,69,55
03131,20070406,62,56
03131,20070407,60,56
03131,20070408,62,57
03131,20070409,66,56
03131,20070410,68,56
03131,20070411,66,55
03131,20070412,62,52
03131,20070413,70,47
03131,20070414,70,49
03131,20070415,59,50
03131,20070416,63,49
03131,20070417,64,50
03131,20070418,62,49
03131,20070419,65,45
03131,20070420,58,49
03131,20070421,61,47
03131,20070422,63,49
03131,20070423,63,53
03131,20070424,72,51
03131,20070425,75,52
03131,20070426,65,56
03131,20070427,71,54
03131,20070428,76,56
03131,20070429,66,57
03131,20070430,69,58

```

## 1.3、练习2

- 编写reduce函数，计算每日最大(最小)“温度”所有气象站。

1. 上传jar包到服务器

将temp-1.0.jar 上传到服务器任意目录

2. 执行MapReduce程序

```shell

# 1. 执行计算最大温度所有气象站程序

hadoop jar temp-1.0.jar com.temp.DayMaxTempStation

# 查看结果
hadoop dfs -cat /user/ylyu001/data/max_temp_station/part-r-00000

日期,气象站,最大温度
20070401,03144,92
20070402,23040,93
20070402,53941,93
20070403,12982,93
20070403,03032,93
20070403,12907,93
20070404,03144,93
20070404,23179,93
20070405,23158,94
20070405,23179,94
20070406,23104,95
20070407,23179,96
20070408,40309,88
20070409,23179,89
20070409,40308,89
20070410,93138,89
20070410,93138,89
20070411,40309,96
20070412,11641,91
20070413,12907,98
20070414,63823,93
20070415,40309,90
20070415,03721,90
20070416,11641,90
20070417,11641,90
20070417,23199,90
20070418,12959,94
20070419,23040,92
20070419,23040,92
20070419,11641,92
20070420,23040,93
20070420,23040,93
20070421,23040,96
20070422,40309,89
20070423,93121,112
20070424,23182,99
20070425,12959,96
20070426,12907,96
20070427,93138,102
20070428,93138,106
20070428,93138,106
20070429,23179,100
20070430,23179,101



# 2. 执行计算最小温度所有气象站程序

hadoop jar temp-1.0.jar com.temp.DayMinTempStation

# 查看结果
hadoop dfs -cat /user/ylyu001/data/min_temp_station/part-r-00000

日期,气象站,最小温度
20070401,26412,-13
20070401,26422,-13
20070402,26422,-13
20070403,27503,-6
20070403,26413,-6
20070404,27515,-15
20070404,27515,-15
20070404,27515,-15
20070405,27515,-17
20070406,27503,-5
20070407,94032,-2
20070407,94032,-2
20070408,27515,-6
20070409,27503,-9
20070409,27406,-9
20070410,27406,-15
20070410,27406,-15
20070411,27515,-14
20070411,27515,-14
20070412,27515,-16
20070412,27406,-16
20070413,27406,-16
20070413,27406,-16
20070413,27406,-16
20070414,27406,-11
20070415,27515,-12
20070416,27503,-13
20070416,27503,-13
20070417,26642,-15
20070418,27515,4
20070418,27515,4
20070418,27406,4
20070418,27406,4
20070418,27406,4
20070419,26625,-4
20070420,26625,-15
20070421,26625,-11
20070422,26625,1
20070423,26625,1
20070424,26642,12
20070424,26601,12
20070424,26601,12
20070424,26601,12
20070425,11640,3
20070426,94902,0
20070426,94902,0
20070426,94902,0
20070426,94902,0
20070426,94902,0
20070426,94902,0
20070427,27503,-1
20070427,27503,-1
20070427,27503,-1
20070428,11640,-11
20070429,27503,-4
20070429,27503,-4
20070429,27503,-4
20070430,11640,-67

```

# 2、 文本聚类

## 2.1、数据准备

```shell script

# 将british-fiction-corpus下面的文件上传到hdfs
# 在 （british-fiction-corpus） 所在的目录执行
hadoop dfs -put british-fiction-corpus/* /user/ylyu001/data/docs

# 查看数据
hadoop dfs -ls /user/ylyu001/data/docs

# 打印出文件列表

```

## 2.2、创建sequence文件

```shell script
# -i : 数据数据在hdfs的路径
# -o : 输出文件路径

mahout seqdirectory -nv -i /user/ylyu001/data/docs -o /user/ylyu001/data/docs-seqdirectory

```

## 2.2、创建稀疏向量

```shell script

# -i : 数据数据在hdfs的路径
# -o : 输出文件路径
mahout seq2sparse -nv -i /user/ylyu001/data/docs-seqfiles -o /user/ylyu001/data/docs-vectorsc

```

## 2.3、计算K-Means近似中心

```shell script

# -i : 数据数据在hdfs的路径
# -o : 输出文件路径
# -ow : 覆盖已存在的结果
# -dm : 距离计算方法

mahout canopy -i /user/ylyu001/data/docs-vectors/tfidf-vectors -ow -o /user/ylyu001/data/docs-vectors/docs-canopy-centroids -dm org.apache.mahout.common.distance.CosineDistanceMeasure -t1 1500 -t2 2000


```


## 2.4、运行K-Means算法, 使用余弦距离

```shell script
# -i : 数据数据在hdfs的路径
# -o : 输出文件路径
# -c : 近似中心
# -dm : 距离计算方法, org.apache.mahout.common.distance.CosineDistanceMeasure(余弦距离) 
# -cd : 收缩值
# -x : 最大迭代次数
# -k :  聚类数量

mahout kmeans -i /user/ylyu001/data/docs-vectors/tfidf-vectors -c /user/ylyu001/data/docs-vectors/docs-canopy-centroids -o /user/ylyu001/data/docs-kmeans-clusters -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cl -cd 0.1 -ow -x 20 -k 5

```

## 2.5、结果评估

```shell script
mahout clusterdump -dt sequencefile -d /user/ylyu001/data/docs-vectors/dictionary.file-0 -i /user/ylyu001/data/docs-kmeans-clusters/clusters-2-final -o clusters.txt -b 100 -p /user/ylyu001/data/docs-kmeans-clusters/clusteredPoints/ -n 20 -e

```

## 2.6、查看评估结果

```shell script
cat clusters.txt

# 结果
Inter-Cluster Density: 0.2632814546210214
Intra-Cluster Density: 0.5713195457480179
CDbw Inter-Cluster Density: 0.0
CDbw Intra-Cluster Density: 1.0138723010057817
CDbw Separation: 1.4576265920362392E7


Inter-Cluster Density: 簇内的紧密度, 越小越好
Intra-Cluster Density: 簇之间的离散度, 越大越好


```


## 2.7、对比不同k值聚类结果

| 指标 | k=5 | k=10 | k=15 |
| ---- | ---- | ---- | ---- | 
|Inter-Cluster Density | 0.596|0.263|0.405 |
|Intra-Cluster Density | 0.616|0.571|0.540 |

> 结论：k=10的时候聚类结果最好


## 2.8、对比不同距离计算方式

### 2.8.1、cosine distance 

```shell script

# 运算K-Means
# -dm org.apache.mahout.common.distance.CosineDistanceMeasure
mahout kmeans -i /user/ylyu001/data/docs-vectors/tfidf-vectors -c /user/ylyu001/data/docs-vectors/docs-canopy-centroids -o /user/ylyu001/data/docs-kmeans-clusters -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cl -cd 0.1 -ow -x 20 -k 10

# 评估结果
mahout clusterdump -dt sequencefile -d /user/ylyu001/data/docs-vectors/dictionary.file-0 -i /user/ylyu001/data/docs-kmeans-clusters/clusters-2-final -o clusters.txt -b 100 -p /user/ylyu001/data/docs-kmeans-clusters/clusteredPoints/ -n 20 -e

# 查看结果
cat clusters.txt

# 结果
Inter-Cluster Density: 0.2632814546210214
Intra-Cluster Density: 0.5713195457480179
CDbw Inter-Cluster Density: 0.0
CDbw Intra-Cluster Density: 1.0138723010057817
CDbw Separation: 1.4576265920362392E7

```

### 2.8.2、Chebyshev distance


```shell script

# 运算K-Means
# -dm org.apache.mahout.common.distance.ChebyshevDistanceMeasure
mahout kmeans -i /user/ylyu001/data/docs-vectors/tfidf-vectors -c /user/ylyu001/data/docs-vectors/docs-canopy-centroids -o /user/ylyu001/data/docs-kmeans-clusters -dm org.apache.mahout.common.distance.ChebyshevDistanceMeasure -cl -cd 0.1 -ow -x 20 -k 10

# 评估结果
mahout clusterdump -dt sequencefile -d /user/ylyu001/data/docs-vectors/dictionary.file-0 -i /user/ylyu001/data/docs-kmeans-clusters/clusters-2-final -o clusters.txt -b 100 -p /user/ylyu001/data/docs-kmeans-clusters/clusteredPoints/ -n 20 -e

# 查看结果
cat clusters.txt


# 结果
Inter-Cluster Density: 0.451973104522727
Intra-Cluster Density: 0.5632747508263757
CDbw Inter-Cluster Density: 0.0
CDbw Intra-Cluster Density: 1.562695315896619
CDbw Separation: 1.45710603449792E7

```


> 结论： cosine distance  距离计算方法聚类效果优于 Chebyshev distance