#include <iostream>

#include "SimpleClient.hpp"
#include "BoundStatementsClient.hpp"


using namespace std;

namespace example
{

const std::string TAGS_COLUMN("tags");
const std::string TITLE_COLUMN("title");
const std::string ARTIST_COLUMN("artist");
const std::string ALBUM_COLUMN("album");

using namespace std;

CassError BoundStatementsClient::loadData()
{
    CassError rc = CASS_OK;
    
    cout << "Loading data into simplex keyspace." << endl;
    
    // prepare the statements
    prepareStatement( "INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?);", &preparedInsertSong );
    prepareStatement( "INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (?, ?, ?, ?, ?);", &preparedInsertPlaylist );
    
    CassUuid songId;
    CassUuid playlistId;
    
    cass_uuid_generate_random(songId);
    cass_uuid_generate_random(playlistId);
    insertSong(songId, "La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker");
    insertPlaylist(playlistId, songId, "La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker");
    cass_uuid_generate_random(songId);
    cass_uuid_generate_random(playlistId);
    insertSong(songId, "Die Mösch", "In Gold", "Willi Ostermann");
    insertPlaylist(playlistId, songId, "Die Mösch", "In Gold", "Willi Ostermann");
    cass_uuid_generate_random(songId);
    cass_uuid_generate_random(playlistId);
    insertSong(songId, "Memo From Turner", "Performance", "Mick Jager");
    insertPlaylist(playlistId, songId, "Memo From Turner", "Performance", "Mick Jager");
    
    return rc;
}

BoundStatementsClient::BoundStatementsClient()
{
    preparedInsertSong = NULL;
    preparedInsertPlaylist = NULL;
}

// private functions

CassError BoundStatementsClient::prepareStatement(const char* query, const CassPrepared** prepared)
{
    CassError rc = CASS_OK;
    cout << "Preparing statement." << endl;
    
    CassFuture* future = cass_session_prepare(getSession(), cass_string_init(query));
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if ( rc != CASS_OK ) {
        cout << "Error preparing insert statement." << endl;
    } else {
        *prepared = cass_future_get_prepared(future);
    }
    cass_future_free(future);
    return rc;
}

CassError BoundStatementsClient::insertSong(CassUuid songId, const char* title, const char* album, const char* artist)
{
    CassError rc = CASS_OK;
    cout << "Inserting song " << title << "." << endl;
    
    CassStatement* statement = cass_prepared_bind(preparedInsertSong);
    CassFuture* future = NULL;
    
    cass_statement_bind_uuid(statement, 0, songId);
    cass_statement_bind_string(statement, 1, cass_string_init(title));
    cass_statement_bind_string(statement, 2, cass_string_init(album));
    cass_statement_bind_string(statement, 3, cass_string_init(artist));
    
    future = cass_session_execute(getSession(), statement);
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cout << "Error inserting row." << endl;
    }
    
    cass_future_free(future);
    cass_statement_free(statement);
    
    return rc;
}

CassError BoundStatementsClient::insertPlaylist(CassUuid playlistId, CassUuid songId, const char* title, const char* album, const char* artist)
{
    CassError rc = CASS_OK;
    cout << "Inserting playlist " << title << "." << endl;
    
    CassStatement* statement = cass_prepared_bind(preparedInsertPlaylist);
    CassFuture* future = NULL;
    
    cass_statement_bind_uuid(statement, 0, playlistId);
    cass_statement_bind_uuid(statement, 1, songId);
    cass_statement_bind_string(statement, 2, cass_string_init(title));
    cass_statement_bind_string(statement, 3, cass_string_init(album));
    cass_statement_bind_string(statement, 4, cass_string_init(artist));
    
    future = cass_session_execute(getSession(), statement);
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cout << "Error inserting song." << endl;
    }
    
    cass_future_free(future);
    cass_statement_free(statement);
    
    return rc;
}

} // end namespace example

/*

      BoundStatement boundStatement = new BoundStatement(statement);
      Set<String> tags = new HashSet<String>();
      tags.add("jazz");
      tags.add("2013");
      getSession().execute(boundStatement.bind(
            UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
            "La Petite Tonkinoise'",
            "Bye Bye Blackbird'",
            "Joséphine Baker",
            tags ) );

*/

