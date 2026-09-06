#include<iostream>
#include<thread>
#include<vector>
#include"minilog.h"
#define ASSERT_TRUE(x) do{if(!(x))\
	{std::cerr<<"[FAIL] "<<#x<<std::endl;\
		return -1;\
	}}while(0)
	//单例是否正常
int TestSingleton()
{
    	Minilog&l1=Minilog::Getinstance();
		Minilog&l2=Minilog::Getinstance();
		ASSERT_TRUE(&l1==&l2);
		std::cout<<"[PASS] TestSingleton"<<std::endl;
		return 0;
}
//边界
// int TestLogNormal(){
// 	Minilog&log=Minilog::Getinstance();
// 	log.getminilevel("DEBUG");
	
// 	log.Print(Levelage::INFO,"test normal msg");
// 	log.Print(Levelage::INFO,std::string(2000,'A'));
// 	std::cout<<"[PASS] TestLogNormal"<<std::endl;
// 	return 0;
// }

int TestMultiThread(){
	Minilog&log=Minilog::Getinstance();
	log.getminilevel("DEBUG");
	log.SetFile("./thread_test.log");
	std::vector<std::thread>thr_vec;
	const int threadcnt=5;
	for(int i=0;i<threadcnt;i++){
		thr_vec.emplace_back([i](){
		for(int j=0;j<20;j++){
       Minilog::Getinstance().Print(Levelage::INFO,"thread"+std::to_string(i)+"msg"+std::to_string(j));
		}
	
	});

}
for(auto&t:thr_vec){
	t.join();
}
std::cout<<"[PASS] TestMultiThread"<<std::endl;
return 0;
}

int main(){
	std::cout<<"===== Start Unit Test ====="<<std::endl;
	int ret=0;
	ret |=TestSingleton();
	
	ret |=TestMultiThread();
	if(ret!=0){
		std::cout<<"==== Some Test FAILED ====="<<std::endl;
		return 1;
	}
	std::cout<<"===== All Test PASS ====="<<std::endl;
	return 0;

}
