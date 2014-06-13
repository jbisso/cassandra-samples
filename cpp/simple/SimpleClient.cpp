#include <iostream>

#include "SimpleClient.hpp"

namespace example {

using namespace std;

const std::string TAGS_COLUMN("tags");
const std::string TITLE_COLUMN("title");
const std::string ARTIST_COLUMN("artist");
const std::string ALBUM_COLUMN("album");

// auxiliary functions

inline CassError printError(CassError error) 
{
    cout << cass_error_desc(error) << "\n";
    return error;
}

inline CassError SimpleClient::executeStatement(const char* cqlStatement, const CassResult* results /* = NULL */)
{
    CassError rc = CASS_OK;
    CassFuture* result_future = NULL;
    
    cout << "Executing " << cqlStatement << "\n";
    
    CassString query = cass_string_init(cqlStatement);
    CassStatement* statement = cass_statement_new(query, 0, CASS_CONSISTENCY_ONE);
    result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);
    
    rc = cass_future_error_code(result_future);
    if (rc == CASS_OK) 
    {
        cout << "Statement " << cqlStatement << " executed successully." << "\n";
        if ( results != NULL )
        {
            results = cass_future_get_result(result_future);
        }
    } 
    else 
    {
        return printError(rc);
    }
    cass_statement_free(statement);
    cass_future_free(result_future);
    
    return rc;
}


CassError SimpleClient::connect(const string nodes) 
{
    CassError rc = CASS_OK;
    
    cout << "Connecting to " << nodes << "\n";
    cluster = cass_cluster_new();
    CassFuture* session_future = NULL;
    const char* contact_points[] = { "127.0.0.1",  NULL };
    const char** contact_point = NULL;
    
    for(contact_point = contact_points; *contact_point; contact_point++)
    {
        cass_cluster_setopt(cluster, CASS_OPTION_CONTACT_POINTS, *contact_point, strlen(*contact_point));
    }

    session_future = cass_cluster_connect(cluster);
    cass_future_wait(session_future);
    rc = cass_future_error_code(session_future);

    if ( rc == CASS_OK )
    {
        cout << "Connected." << "\n";
    }
    else
    {
        return printError(rc);
    }
    
    session = cass_future_get_session(session_future);
    return rc;
}

CassError SimpleClient::createSchema() 
{
    CassError rc = CASS_OK;
    
    cout << "Creating simplex keyspace." << endl;
    rc = executeStatement("CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
    rc = executeStatement("CREATE TABLE simplex.songs (id uuid PRIMARY KEY,title text,album text,artist text,tags set<text>,data blob);");
    rc = executeStatement("CREATE TABLE simplex.playlists (id uuid,title text,album text,artist text,song_id uuid,PRIMARY KEY (id, title, album, artist));");
    
    return rc;
}

CassError SimpleClient::loadData() 
{
    CassError rc = CASS_OK;
    
    cout << "Loading data into simplex keyspace." << endl;
    
    rc = executeStatement("INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50,'La Petite Tonkinoise','Bye Bye Blackbird','Joséphine Baker',{'jazz', '2013'});");
    rc = executeStatement("INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (f6071e72-48ec-4fcb-bf3e-379c8a696488,'Die Mösch','In Gold','Willi Ostermann',{'kölsch', '1996', 'birds'});");
    rc = executeStatement("INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,'Memo From Turner','Performance','Mick Jager',{'soundtrack', '1991'});");
    
    rc = executeStatement("INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,756716f7-2e54-4715-9f00-91dcbea6cf50,'La Petite Tonkinoise','Bye Bye Blackbird','Joséphine Baker');");
    rc = executeStatement("INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,f6071e72-48ec-4fcb-bf3e-379c8a696488,'Die Mösch','In Gold','Willi Ostermann');");
    rc = executeStatement("INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (3fd2bedf-a8c8-455a-a462-0cd3a4353c54,fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,'Memo From Turner','Performance','Mick Jager');");
    
    return rc;
}

CassError SimpleClient::querySchema()
{
    CassError rc = CASS_OK;
    CassResult* results;
    
    cout << "Querying the simplex.playlists table." << endl;
    
    rc = executeStatement("SELECT title, artist, album FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;", results);
    
    CassIterator* rows = cass_iterator_from_result(results);
    
    if ( rc == CASS_OK ) 
    {
        while ( cass_iterator_next(rows) ) 
        {
            const CassRow* row = cass_iterator_get_row(rows);
            
            CassString title, artist, album;
            
            cass_value_get_string(cass_row_get_column(row, 0), &title);
            cass_value_get_string(cass_row_get_column(row, 1), &artist);
            cass_value_get_string(cass_row_get_column(row, 2), &album);
            
            cout << "title: " << title.data << " artist: " << artist.data << " album:" << album.data << "\n";
        }
        cass_result_free(results);
        cass_iterator_free(rows);
    }
    
    return rc;
}

/*
void SimpleClient::updateSchema() 
{
    cout << "Updating the simplex.songs table." << endl;
    shared_ptr<cql::cql_query_t> update_songs_statement(new cql::cql_query_t(
        string("UPDATE simplex.songs ") +
            "SET tags = tags + { 'entre-deux-guerres' } " +
            "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;"
            ));
    boost::shared_future<cql::cql_future_result_t> future = session->query(update_songs_statement);
    future.wait();
    
    cout << "Querying the simplex.songs table." << endl;
    shared_ptr<cql::cql_query_t> query_songs_statement(new cql::cql_query_t(
        string("SELECT * FROM simplex.songs ") +
        "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;"
        ));
    future = session->query(query_songs_statement);
    future.wait();
    shared_ptr<cql::cql_result_t> result = future.get().result;
    cout << TITLE_COLUMN << "\t" << ALBUM_COLUMN << "\t" << ARTIST_COLUMN << "\t" << TAGS_COLUMN << endl;
    if ( result ) 
    {
        while ( result->next() ) 
        {
            string title, artist, album, tag;
            cql::cql_set_t* tags;
            result->get_string(TITLE_COLUMN, title);
            result->get_string(ALBUM_COLUMN, artist);
            result->get_string(ARTIST_COLUMN, album);
            cout << title << "\t" << album << "\t" << artist << "\t" << "{";
            if (result->get_set(TAGS_COLUMN, &tags) )
            {
                for ( int i = 0; i < tags->size(); ++i )
                {
                    cout << " " << tags->get_string(i, tag);
                }
            }
            cout << " }" << endl;
        }
    }
}
*/

CassError SimpleClient::dropSchema() 
{
    CassError rc = CASS_OK;
    CassFuture* result_future = NULL;
    
    cout << "Dropping simplex keyspace." << "\n";
    CassString query = cass_string_init("DROP KEYSPACE simplex;");
    CassStatement* statement = cass_statement_new(query, 0, CASS_CONSISTENCY_ONE);
    result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);
    
    rc = cass_future_error_code(result_future);
    if (rc != CASS_OK) 
    {
        return printError(rc);
    }
    cass_statement_free(statement);
    cass_future_free(result_future);
    return rc;
}

void SimpleClient::close() 
{
    cout << "Closing down cluster connection." << "\n";
    cass_cluster_free(cluster);
}

} // end namespace example




