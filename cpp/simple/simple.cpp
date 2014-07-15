#include <iostream>

#include "SimpleClient.hpp"

int main(int argc, char**)
{
    using example::SimpleClient;
    
    SimpleClient client;
    client.connect("127.0.0.1");
    client.createSchema();
    client.loadData();
    client.querySchema();
    client.updateSchema();
    std::cout << "\nPress <Return> to continue.\n";
    std::getchar();
    
    client.dropSchema();
    client.close();
    return 0;
}

