#ifndef __BOUND_STATEMENTS_CLIENT_H__
#define __BOUND_STATEMENTS_CLIENT_H__


namespace example
{

class BoundStatementsClient : public SimpleClient
{

public:    
    virtual void loadData();
    
    BoundStatementsClient();
    ~BoundStatementsClient() { }
    

}; // end class BoundStatementsClient

} // end namespace example

#endif

