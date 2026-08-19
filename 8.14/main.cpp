#include"minilog.h"
#include"thread"
void ThreadFunc(){
    for(int i=0;i<500;i++){
        LOG_D("子线程日志 num:"+std::to_string(i));
    }
}

int main(){
    
    Minilog::Getinstance().SetFile("run.log");
    Minilog::Getinstance().setmaxsize(100);
    LOG_D("程序启动");
    std::thread t1(ThreadFunc);
    std::thread t2(ThreadFunc);
    t1.join();
    t2.join();
    LOG_D("程序结束");
    return 0;
}