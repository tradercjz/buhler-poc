g++ qps.cpp -std=c++11 -DGLIBC_USE_CXX11_ABI=0 -I./include -L. -lDolphinDBAPI -lpthread -lssl -lrt -O3 -o qps
g++ mtw.cpp -std=c++11 -DGLIBC_USE_CXX11_ABI=0 -I./include -L. -lDolphinDBAPI -lpthread -lssl -lrt -O3 -o mtw