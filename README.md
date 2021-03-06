# kafka2delta
speed up merge of delta table by kafka

# 背景

Delta是個強大的工具，但聰明的人類總能找到讓Delta尷尬的情境，例如常常需要更新資料，因此要執行大量且昂貴的`MERGE INTO`

# 解法

我們利用kafka來優化merge的資料流：

1. 選定一個timestamp欄位來將資料分組到不同的kafka partition
2. 使用kafka compact topic來盡可能de-duplicate

測試三千萬資料時，此解法將資料處理所需的時間從20分鐘降低到9分鐘左右

# 測試所需

1. docker (所有的程式碼都是放在容器中執行)
2. curl
3. 對外網路（需要下載docker image)

# 測試

1. 下載此專案

```shell
git clone https://github.com/chia7712/kafka2delta
cd kafka2delta
```

2. 建立kafka集群
```shell
./docker/start_kafka_cluster.sh
```

3. 執行`csv to kafka`任務：
```shell
export ROOT_FOLDER=$HOME/kafka2delta \
&& $ROOT_FOLDER/docker/submit_write_kafka.sh \
  --brokers 192.168.50.178:12015
```
> - 請記得更改broker的位址
> - metadata下的檔案決定了資料schema和分佈，請見後面說明

4. 執行`kafka to delta` (資料將輸出到本地目錄) 任務：

```shell
export ROOT_FOLDER=$HOME/kafka2delta \
&& $ROOT_FOLDER/docker/submit_write_delta.sh \
  --path /tmp/chia2 \
  --brokers 192.168.50.178:12015 \
  --master "local[*]"
```
> - 請記得更改broker的位址 

# 將kafka的資料輸出到azure gen2

請先取得gen2 access key，然後如下設定`gen2 account`, `gen2 container`, `gen2 access key`

```shell
export ROOT_FOLDER=$HOME/kafka2delta \
&& $ROOT_FOLDER/docker/submit_write_delta.sh \
  --account {your_account} \
  --container {your_container} \
  --key {you_key} \
  --path chia \
  --brokers 192.168.50.178:12015 \
  --master "local[*]"
```
> - 請記得更改broker的位址

# metadata

```xml
<tableInfos>
    <table name="table">
        <csvFolder>table</csvFolder>
        <topic>table</topic>
        <deltaFolder>table</deltaFolder>
        <columns>c1,c2,c3,c4,c5,c6,c7,c8,c9,c10</columns>
        <pks>c1</pks>
        <types>int,timestamp,timestamp,timestamp,str,str,str,str,str,str</types>
        <partitionBy>c2</partitionBy>
        <orderBy>c3,c4</orderBy>
        <partitions>10</partitions>
        <compact>true</compact>
    </table>
</tableInfos>
```

參數           | 說明
--------------|:---------------------------------------------------------------------------------------------------------------
csvFolder     | 用在`csv to kafka`，代表csv檔案所在的（相對）目錄。注意：根目錄是由`csv to kafka`提交任務時所指定(`--csv $ROOT_FOLDER/csv`)
topic         | 用在`csv to kafka`，代表csv檔案的資料要放到哪一個kafka topic
deltaFolder   | 用在`kafka to delta`，代表kafka topic的資料要輸出的目錄。注意：根目錄是由`kafka to delta`提交任務時所指定(`--path chia`)
columns       | 欄位名稱。注意：該些名稱會自動轉成小寫
pks           | primary key名稱。注意：該些名稱會自動轉成小寫
types         | 欄位型別。如果省略的話所有欄位都會設定成字串
partitionBy   | 用來分組的欄位。此值會影響資料在delta內的分佈
orderBy       | 用來去重的欄位。當輸入CSV檔案時，重複的資料會依照此欄位排序後將舊的資料刪掉
partitions    | kafka partitions的數量
compact       | 是否要啟用kafka compact功能

# 平行度

1. 部署真的kafka叢集，並且提高上述的`partitions`，如此可增加資料來源的吞吐量
2. 部署真的spark叢集，並將上述的`mode`改成spark分散式環境下的位址，如此可讓多個節點一起處理資料
3. `kafka2delta/metadata`下每一個檔案會提交一個spark application。可以新增多個檔案以提交多個spark application處理多個資料表格