# 这是一个mongodb数据库实时同步的脚本

## 需要python3支持

### 下载依赖库
```shell
pip3 install pymongo
```

### 运行
```shell
nohub python3 -u mongodump.py -s "mongodb://source" -d "mongodb://dest" > log.txt &
```