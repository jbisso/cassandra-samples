#include <iostream>

#include "SimpleClient.hpp"
#include "BoundStatementsClient.hpp"

inline void wait_on_enter()
{
    std::string dummy;
    std::cout << "Enter to continue..." << std::endl;
    std::getline(std::cin, dummy);
}

int main(int argc, char**)
{
    using example::SimpleClient;
    using example::BoundStatementsClient;
    
    BoundStatementsClient client;
    client.connect("127.0.0.1");
    client.createSchema();
    client.loadData();
    wait_on_enter();
    client.querySchema();
    client.updateSchema();
    client.dropSchema("simplex");
    client.close();
    return 0;
}

