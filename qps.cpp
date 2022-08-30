#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include "DolphinDB.h"
#include "Util.h"

using namespace std;
using namespace dolphindb;

class QueryStmts {
    public:
        QueryStmts() {};
    private:
        vector<string> stms;
};

class Query {
    static string HOST;
    static int PORT;
    static string USER;
    static string PASSWORD;

    public:
        Query(int minutes, string tableName): minutes(minutes), tableName(tableName) {
            cout << "tbName:" << tableName << endl;
        };

        Query(const Query& q) {
            minutes = q.minutes;
            tableName = q.tableName;
            okNum = q.okNum;
            errNum = q.errNum;
        }

        ~Query() {if(initSuccess) {conn.close();}};
        bool init();

        void run(int *rval1, int *rval12);
    private:
        int okNum = 0;
        int errNum = 0;
        int minutes;
        string tableName;

        DBConnection conn;
        bool initSuccess = false;
};

string Query::HOST = "127.0.0.1";
int Query::PORT = 3302;
string Query::USER = "admin";
string Query::PASSWORD = "123456";

bool Query::init() {
    initSuccess = conn.connect(HOST, PORT, USER, PASSWORD);
    if(!initSuccess) {
        cout <<"Failed to conenct to the server"<<endl;
        return false;
    }
    return true;
}

// streamTable

void Query::run(int *rval1, int *rval2) {
    if(!init()) return;

    using namespace std::chrono;
    long long startTime = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
    long long endTime = startTime + minutes * 60 * 1000;
    int index =0 ;
    do {
        try {
            int randIndex = index%50+1;
            int randDevId = index%6+1;
            TableSP t = conn.run("select last(value) from "+tableName+to_string(randIndex)+" where deviceId='dev"+to_string(randDevId)+"'");
            int ct = t.count();
            if (ct < 1) {
                cout << "ct less "<< ct << endl;
            } else {
               
            }
            okNum ++;
        } catch (exception &e) {
            cout << e.what() << endl;
            return ;
            errNum ++;
        }
        
        startTime = duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
        ++index;
    } while(startTime < endTime);


    *rval1 = okNum;
    *rval2 = errNum;
}

int main(int argc, char *argv[]) {
    vector<Query> qrys;
    vector<thread> ths;
    vector<string> tableNames = {"FloatSTable", "LongSTable", "StringSTable"};
    int len = tableNames.size();
    
    
    if(argc < 3) {
        cerr << "Usage: " << argv[0] << " <numThreads> <runningTime>" << endl;
        return -1;
    }

    int numThreads = atoi(argv[1]);
    int minutes = atoi(argv[2]);

    cout << "numThreads: "<< numThreads << ",minutes: " << minutes << endl;

    vector<int> okNums(numThreads, 0);
    vector<int> errNums(numThreads, 0);

    random_device rd;
    default_random_engine eng(rd());
    uniform_int_distribution<int> distr(0, len-1);
   
    for(int i = 0; i < numThreads; i++) {
        Query q {minutes, tableNames[distr(eng)]};
        qrys.emplace_back(q);
    }

    for(int i = 0; i < numThreads; i++) {
        thread tmp(&Query::run,qrys[i], &okNums[i], &errNums[i]);
        ths.emplace_back(move(tmp));
    }

    for(int i = 0; i < numThreads; i++) {
        ths[i].join();
    }

    unsigned int cnts = 0;
    for(int i = 0; i < okNums.size(); ++i) {
        cnts += okNums[i];
    }

    unsigned int errs = 0;
     for(int i = 0; i < okNums.size(); ++i) {
        errs += errNums[i];
    }

    cout << "QPS:" << (double)cnts/minutes/60 << endl;
    cout << "oks:" << cnts << endl;
    cout << "errs:" << errs << endl;
    return 0;
}