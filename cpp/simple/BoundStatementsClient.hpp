#ifndef __BOUND_STATEMENTS_CLIENT_H__
#define __BOUND_STATEMENTS_CLIENT_H__


namespace example
{

class BoundStatementsClient : public SimpleClient
{
private:
    static final String INSERT_SONGS_DATA_PREPARED = 
        "INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?);";
    static final String INSERT_PLAYLISTS_DATA_PREPARED = 
        "INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (?, ?, ?, ?, ?);";
    PreparedStatement insertSongsDataStatement;
    PreparedStatement insertPlaylistsDataStatement;

public:
    prepareStatements();
    virtual void loadData();
    
    BoundStatementsClient();
    ~BoundStatementsClient() { }
    

}; // end class BoundStatementsClient

} // end namespace example

#endif

