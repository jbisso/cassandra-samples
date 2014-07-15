#ifndef __SIMPLE_CLIENT_H__
#define __SIMPLE_CLIENT_H__

#include <string>

#include <cassandra.h>

namespace example 
{

class SimpleClient 
{

private:
    CassSession* session;
    CassCluster* cluster;
    inline CassError executeStatement(const char* cqlStatement, const CassResult** results = NULL);
public:
    inline CassSession* getSession() { return session; }
    
    CassError connect(const std::string nodes);
    CassError createSchema();
    virtual CassError loadData();
    CassError querySchema();
    CassError updateSchema();
    CassError dropSchema();
    void close();
    
    SimpleClient() { }
    ~SimpleClient() { }
    
};
    
} // end namespace example
 
#endif


