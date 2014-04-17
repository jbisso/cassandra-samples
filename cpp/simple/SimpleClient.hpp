#ifndef __SIMPLE_CLIENT_H__
#define __SIMPLE_CLIENT_H__

#include <string>

#include <boost/asio.hpp>
#include <cql/cql.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_result.hpp>

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


