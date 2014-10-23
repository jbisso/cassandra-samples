#ifndef __BOUND_STATEMENTS_CLIENT_H__
#define __BOUND_STATEMENTS_CLIENT_H__

#include "cassandra.h"

namespace example
{

class BoundStatementsClient : public SimpleClient
{
private:
    const CassPrepared* preparedInsertSong;
    const CassPrepared* preparedInsertPlaylist;
    
    CassError prepareStatement(const char* query, const CassPrepared** prepared);
    CassError insertSong(CassUuid id, const char* title, const char* album, const char* artist);
    CassError insertPlaylist(CassUuid playlistId, CassUuid songId, const char* title, const char* album, const char* artist);
public:    
    virtual CassError loadData();
    
    BoundStatementsClient();
    ~BoundStatementsClient() { }
    

}; // end class BoundStatementsClient

} // end namespace example

#endif

