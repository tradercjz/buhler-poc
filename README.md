
## 文件说明

mtw.cpp: 模拟数据写入

qps.cpp: 近期数据（流表）QPS查询性能测试

include: DolphinDB头文件

libDolphinDBAPI.so：DolphinDB动态库

make.sh: 编译命令

mtw: 模拟数据写入程序

qps: QPS查询程序

dos目录：dolphindb脚本目录

dos/create.dos: 建库建表，创建流表和订阅

dos/clean.dos: 删除订阅，流表和DFS库表

dos/check.dos: 判断流表数据量和DFS的数据量一致


如果执行mtw/qps程序，遇到“error while loading shared libraries: libDolphinDBAPI.so: cannot open shared objec“这个报错，可以执行
"export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./"，即设置下LD环境变量，然后执行就可以了