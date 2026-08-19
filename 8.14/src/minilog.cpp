#include"minilog.h"

static std::string getlevel(Levelage level){
      switch (level)
      {
      case Levelage::DEBUG:return "DEBUG";
        break;
       case Levelage::INFO:return "INFO";
        break;
        case Levelage::WARN:return "WARN";
        break;
        case Levelage::ERROR:return "ERROR";
        break;
      default:return "DONTNT";
        break;
      }
}
Minilog&Minilog::Getinstance(){
    static Minilog instance;
    return instance;
}
void Minilog::SetFile(const std::string&path){
    std::lock_guard<std::mutex> lock(mmtx);
    
    if(mofs.is_open()){
        mofs.close();
    }
    m_log_path=path;
    mofs.open(m_log_path,std::ios::out|std::ios::app);
    if(mofs.is_open()){
        menablefile=true;
    }else{
        menablefile=false;
    }
}
void Minilog::checkroll(){

if(!menablefile)return;
std::ifstream tmp(m_log_path,std::ios::in|std::ios::ate);
 if(!tmp.is_open()){
    return;
 }
 std::streampos size=tmp.tellg();
 if((size_t)size<m_roll_bytes){
    return;
 }
 mofs.close();
 size_t index=1;
 while(true){
    std::string check_name=m_log_path+"_"+std::to_string(index);
    std::ifstream file(check_name);
    if(file.is_open()){
        index++;
        file.close();
    }else{
        break;
    }
 }
 std::string backup_name=m_log_path+"_"+std::to_string(index);
 int ret=std::rename(m_log_path.c_str(),backup_name.c_str());
 if(ret!=0){
    std::cout<<"fail"<<std::endl;
 }
 mofs.open(m_log_path,std::ios::out|std::ios::app);
 if(mofs.is_open()){
    menablefile=true;
 }
}
void Minilog::setmaxsize(size_t x){
    std::lock_guard<std::mutex> lock(mmtx);
    m_roll_bytes=x;
}
void Minilog::getminilevel(const std::string&level){
    minilevel=level;
}
void Minilog::Print(Levelage level,const std::string&msg){
     std::lock_guard<std::mutex> lock(mmtx);
      checkroll();
     std::stringstream ss;
     std::string l=getlevel(level);
     getminilevel("DEBUG");
      if(l<minilevel){
        return;
      }
     
     ss<<"["<<Gettime()<<"]"<<msg<<"["<<minilevel<<"]";
     std::string log_str=ss.str();
     std::cout<<log_str<<std::endl;
     if(menablefile){
        mofs<<log_str<<std::endl;
        mofs.flush();
     }
}
std::string Minilog::Gettime(){
   
    std::time_t t=std::time(nullptr);
    return std::ctime(&t);
}
Minilog::~Minilog(){
    if(mofs.is_open()){
        mofs.close();
    }
}