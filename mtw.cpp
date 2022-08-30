#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <type_traits>
#include <future>
#include "DolphinDB.h"
#include "Util.h"

using namespace std;
using namespace dolphindb;

//模拟数据生成，对于1个工厂，100个设备，每个设备20个点位，每个点位1条/s，需要模拟2000个数据点的写入
//50个工厂，通过50个线程来处理
class MockFactor {
    public:
        MockFactor(int numDevices = 100, int numPoint = 20, int numFloatPoint = 6, int numLongPint = 6):
         numDevices(numDevices), numPoint(numPoint), numFloatPoint(numFloatPoint), numLongPoint(numLongPint), numStringPoint(numPoint - numLongPoint -numLongPoint ) {};
        TableSP mockOneSecData();
        TableSP mockLongData();
        TableSP mockStringData();
        TableSP mockFloatData();
        //void run();
    private:
        //工厂设备数量
        int numDevices;
        //每个设备的点位数量
        int numPoint;
        //3种类型的表，每种类型测点数量： numPoint = numLongPoint + numLongPoint + numStringPoint
        // 小数型表，默认为6个测点
        int numFloatPoint;
        // 整数型表，默认为6个测点
        int numLongPoint;
        // 字符串型表，默认为8个测点
        int numStringPoint;
};

TableSP MockFactor::mockFloatData() {
    vector<string> colNames = {"measurement","time","coefficient","coefficientext","decimalplaces","deviceId","displayname","displaytype","id","label","originvalue","type","typename","value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_NANOTIMESTAMP, DT_FLOAT,DT_FLOAT,DT_INT,DT_SYMBOL,DT_STRING, DT_STRING, DT_LONG, DT_STRING, DT_FLOAT, DT_SYMBOL, DT_SYMBOL, DT_FLOAT};
    int colNum = 14, rows = numDevices * numFloatPoint;
    ConstantSP table = Util::createTable(colNames, colTypes, rows, rows);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));
    
    int index = 0;
    using namespace std::chrono;
    long long  startTime = duration_cast< nanoseconds >(system_clock::now().time_since_epoch()).count();
    for(int i = 0; i < numDevices; i++) {
        for(int j = 0; j < numFloatPoint; j++) {
            index = i * numFloatPoint + j;
            columnVecs[0]->set(index, Util::createString("measure"+to_string(j)));
            columnVecs[1]->setLong(index, startTime + i*100000);
            columnVecs[2]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[3]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[4]->set(index, Util::createInt(rand()%100));
            columnVecs[5]->set(index, Util::createString("dev"+to_string(i)));
            columnVecs[6]->set(index, Util::createString("deve_"+std::to_string(i)));
            columnVecs[7]->set(index, Util::createString("dilay_"+std::to_string(i)));
            columnVecs[8]->set(index, Util::createLong(rand()%100));
            columnVecs[9]->set(index, Util::createString("label_"+std::to_string(i)));
            columnVecs[10]->set(index, Util::createFloat(rand()%100));
            columnVecs[11]->set(index, Util::createString("type_"+std::to_string(i)));
            columnVecs[12]->set(index, Util::createString("typeName_"+std::to_string(i)));
            columnVecs[13]->set(index, Util::createFloat((rand()%100)/3.0));
        }
    }

    return table;
}

TableSP MockFactor::mockStringData() {
    vector<string> colNames = {"measurement","time","coefficient","coefficientext","decimalplaces","deviceId","displayname","displaytype","id","label","originvalue","type","typename","value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_NANOTIMESTAMP, DT_FLOAT,DT_FLOAT,DT_INT,DT_SYMBOL,DT_STRING, DT_STRING, DT_LONG, DT_STRING, DT_FLOAT, DT_SYMBOL, DT_SYMBOL, DT_STRING};
    int colNum = 14, rows = numDevices * numStringPoint;
    ConstantSP table = Util::createTable(colNames, colTypes, rows, rows);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));
    
    int index = 0;
    using namespace std::chrono;
    long long  startTime = duration_cast< nanoseconds >(system_clock::now().time_since_epoch()).count();
    for(int i = 0; i < numDevices; i++) {
        for(int j = 0; j < numStringPoint; j++) {
            index = i * numStringPoint + j;
            columnVecs[0]->set(index, Util::createString("measure"+to_string(j)));
            columnVecs[1]->setLong(index, startTime + i*100000);
            columnVecs[2]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[3]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[4]->set(index, Util::createInt(rand()%100));
            columnVecs[5]->set(index, Util::createString("dev"+to_string(i)));
            columnVecs[6]->set(index, Util::createString("deve_"+std::to_string(i)));
            columnVecs[7]->set(index, Util::createString("dilay_"+std::to_string(i)));
            columnVecs[8]->set(index, Util::createLong(rand()%100));
            columnVecs[9]->set(index, Util::createString("label_"+std::to_string(i)));
            columnVecs[10]->set(index, Util::createFloat(rand()%100));
            columnVecs[11]->set(index, Util::createString("type_"+std::to_string(i)));
            columnVecs[12]->set(index, Util::createString("typeName_"+std::to_string(i)));
            columnVecs[13]->set(index, Util::createString("value_"+std::to_string(i)));
        }
    }

    return table;
}

TableSP MockFactor::mockLongData() {
    vector<string> colNames = {"measurement","time","coefficient","coefficientext","decimalplaces","deviceId","displayname","displaytype","id","label","originvalue","type","typename","value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_NANOTIMESTAMP, DT_FLOAT,DT_FLOAT,DT_INT,DT_SYMBOL,DT_STRING, DT_STRING, DT_LONG, DT_STRING, DT_FLOAT, DT_SYMBOL, DT_SYMBOL, DT_LONG};
    int colNum = 14, rows = numDevices * numFloatPoint;
    ConstantSP table = Util::createTable(colNames, colTypes, rows, rows);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));
    
    int index = 0;
    using namespace std::chrono;
    long long  startTime = duration_cast< nanoseconds >(system_clock::now().time_since_epoch()).count();
    for(int i = 0; i < numDevices; i++) {
        for(int j = 0; j < numFloatPoint; j++) {
            index = i * numFloatPoint + j;
            columnVecs[0]->set(index, Util::createString("measure"+to_string(j)));
            columnVecs[1]->setLong(index, startTime + i*100000);
            columnVecs[2]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[3]->set(index, Util::createFloat((rand()%100)/3.0));
            columnVecs[4]->set(index, Util::createInt(rand()%100));
            columnVecs[5]->set(index, Util::createString("dev"+to_string(i)));
            columnVecs[6]->set(index, Util::createString("deve_"+std::to_string(i)));
            columnVecs[7]->set(index, Util::createString("dilay_"+std::to_string(i)));
            columnVecs[8]->set(index, Util::createLong(rand()%100));
            columnVecs[9]->set(index, Util::createString("label_"+std::to_string(i)));
            columnVecs[10]->set(index, Util::createFloat(rand()%100));
            columnVecs[11]->set(index, Util::createString("type_"+std::to_string(i)));
            columnVecs[12]->set(index, Util::createString("typeName_"+std::to_string(i)));
            columnVecs[13]->set(index, Util::createInt(rand()%100));
        }
    }

    return table;
}

// 每个工厂一个线程，模拟生成数据
class Runner {
    static string HOST;
    static int PORT;
    static string USER;
    static string PASSWORD;

    public:
        Runner( int tbId, int seconds = 10):  tbId(tbId),seconds(seconds){};
        ~Runner() {if(initSuccess) {conn.close();}};

        Runner(const Runner &r) {
            tbId = r.tbId;
            seconds = r.seconds;
        }
        void run(promise<int>& p);
        bool init();
    private:
        MockFactor mk{100};
        int tbId;
        //运行时长
        int seconds;

        DBConnection conn;
        bool initSuccess = false;
       
};

bool Runner::init() {
    vector<string> sites {"192.193.168.1:3302", "192.193.168.2:3302", "192.193.168.3:3302"};
    initSuccess = conn.connect(HOST, PORT, USER, PASSWORD, "", true, sites);
    if(!initSuccess) {
        cout <<"Failed to conenct to the server"<<endl;
        return false;
    }
    return true;
}

string Runner::HOST = "192.193.168.2";
int Runner::PORT = 3302;
string Runner::USER = "admin";
string Runner::PASSWORD = "123456";

void Runner::run(promise<int>& p) {
    init();

    using namespace std::chrono;
    long long currTime = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
    long long endTime = currTime + seconds * 1000;
    int okNum = 0;
    int errNum = 0;
    thread_local int rows = 0;
    do {
        try {
            // 模拟生成字符串型数据表
            TableSP st = mk.mockStringData();
            vector<ConstantSP> args;
            args.push_back(st);
            ConstantSP csp = conn.run("tableInsert{StringSTable"+to_string(tbId)+"}", args);

            rows += csp->get(0)->getInt();

            // 模拟生成小数型数据表
            TableSP ft = mk.mockFloatData();
            vector<ConstantSP> args2;
            args2.push_back(ft);
            csp = conn.run("tableInsert{FloatSTable"+to_string(tbId)+"}", args2);
            rows += csp->get(0)->getInt();

            // 模拟生成整数型数据表
            TableSP lt = mk.mockLongData();
            vector<ConstantSP> args3;
            args3.push_back(lt);
            csp = conn.run("tableInsert{LongSTable"+to_string(tbId)+"}", args3);
            rows += csp->get(0)->getInt();

            okNum ++;
        } catch (exception &e) {
            errNum ++;
            cout << "ERROR:" << e.what() << endl;
        }

        long long currTime2 = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
        long elapsed = currTime2 - currTime;
        if (elapsed < 980) {
            // 不足1秒，则延时等待
            cout << "sleeping:" << 980 - elapsed << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(980 - elapsed));
        } else {
            // 实际测试中，没有下面输出，数据都是在1秒内完成构造并写入
            cout << "takes:" << elapsed << endl;
        }
        currTime =  duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
    } while (currTime < endTime);

    p.set_value(rows);
}

int main(int argc, char *argv[]) {
    if(argc < 3) {
        cerr << "Usage: " << argv[0] << " <numThreads> <runningTime>" << endl;
        return -1;
    }

    int numFactory = atoi(argv[1]);
    int minutes = atoi(argv[2]);

    vector<Runner> allRunner;
    vector<thread> ths;

    vector<promise<int>> promises;
    for(int i = 1; i <= numFactory; i++) {
        Runner tmp {i, minutes*60};
        allRunner.emplace_back(tmp);
        promise<int> p;
        promises.emplace_back(move(p));
    }

    for(int i = 0; i < numFactory; i++) {
        thread tmp(&Runner::run, &allRunner[i], ref(promises[i]));
        ths.emplace_back(move(tmp)); 
    }

    for(int i = 0; i < numFactory; i++) {
        ths[i].join();
    }

    for(int i = 0; i < numFactory; i++) {
        cout << "thread" << i <<" writes: " << promises[i].get_future().get() << endl;
    }

    return 0;
}