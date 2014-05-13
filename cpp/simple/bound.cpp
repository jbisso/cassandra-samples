#include "SimpleClient.hpp"
#include "BoundStatementsClient.hpp"

int main(int argc, char**)
{
    using example::SimpleClient;
    using example::BoundStatementsClient;
    
    BoundStatementsClient client;
    client.connect("127.0.0.1");
    client.createSchema();
    client.loadData();
    client.querySchema();
    client.updateSchema();
    client.dropSchema("simplex");
    client.close();
    return 0;
}

