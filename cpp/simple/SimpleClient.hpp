#ifndef __SIMPLE_CLIENT_H__
#define __SIMPLE_CLIENT_H__

#include <string>

#include <cassandra.h>

namespace example 
{

class SimpleClient 
{

private:
    boost::shared_ptr<cql::cql_session_t> session;
    boost::shared_ptr<cql::cql_cluster_t> cluster;
public:
    inline boost::shared_ptr<cql::cql_session_t> getSession() { return session; }
    
    void connect(const std::string nodes);
    void createSchema();
    virtual void loadData();
    void querySchema();
    void updateSchema();
    void dropSchema(const std::string schema);
    void close();
    
    SimpleClient();
    ~SimpleClient() { }
    
};
    
} // end namespace example
 
#endif


