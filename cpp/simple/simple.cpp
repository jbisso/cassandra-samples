#include <iostream>

#include "SimpleClient.hpp"

inline void wait_on_enter()
{
    std::string dummy;
    std::cout << "Enter to continue..." << std::endl;
    std::getline(std::cin, dummy);
}

int main(int argc, char**)
{
    using example::SimpleClient;
    
    SimpleClient client;
    client.connect("127.0.0.1");
    client.createSchema();
    client.loadData();
    client.querySchema();
    client.updateSchema();
    wait_on_enter();
    client.dropSchema("simplex");
    client.close();
    return 0;
}

