#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<mysql/mysql.h>
#include<string>
#include"minilog.h"
class Transaction{
    private:
     MYSQL*conn_;
     bool committed_=false;
     bool rollbacked_=false;
   public:
    explicit Transaction(MYSQL*conn):conn_(conn){
        if(mysql_query(conn_,"START TRANSACTION")!=0){
            LOG_E("START TRANSACTION failed:%s",mysql_error(conn_));
        }
    }
    ~Transaction(){
        if(!committed_&&!rollbacked_){
            rollbacked();
        }
    }
    bool commit(){
        if(mysql_query(conn_,"COMMIT")!=0){
            LOG_E("COMMIT failed:%s",mysql_error(conn_));
            return false;
        }
        committed_=true;
        return true;
    }
   void rollback(){
    if(!committed_){
        mysql_query(conn_,"ROLLBACK");
        rollbacked_=true;
    }
   } 
   Transaction(const Transaction&)=delete;
   Transaction&operator=(const Transaction&)=delete;
       
};
#endif