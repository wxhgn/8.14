#pragma once
#include<mutex>
#include<iostream>
#include<string>
#include<chrono>
#include<sstream>
#include<ctime>
#include<fstream>
enum class Levelage{
    DEBUG,
    INFO,
    WARN,
    ERROR,
};
class Minilog{
    private:
       Minilog()=default;
       ~Minilog();
       Minilog(const Minilog&)=delete;
       Minilog&operator=(const Minilog&)=delete;
       std::string Gettime();
       std::mutex mmtx;
       std::string m_log_path;
       size_t m_roll_bytes=5*1024*1024;
       std::ofstream mofs;
       std::string minilevel="DEBUG";
       bool menablefile=false;
    public:
    void setmaxsize(size_t x);
    void checkroll();
      static Minilog&Getinstance();   
      void SetFile(const std::string&path);
      void Print(Levelage level,const std::string&msg);
      void getminilevel(const std::string&level);
};
#define LOG_D(msg) Minilog::Getinstance().Print(Levelage::DEBUG,msg)