#include <iostream>

#include "SimpleClient.hpp"

#include <cql/cql_execute.hpp>
#include <cql/cql_set.hpp>

#include <cstdio>

namespace example {

using namespace std;
using namespace cql;
using boost::shared_ptr;

const std::string TAGS_COLUMN("tags");
const std::string TITLE_COLUMN("title");
const std::string ARTIST_COLUMN("artist");
const std::string ALBUM_COLUMN("album");


SimpleClient::SimpleClient() 
{
    cql_initialize();
}

void SimpleClient::connect(const string nodes) 
{
    cout << "Connecting to " << nodes << "\n";
    shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
    builder->add_contact_point(boost::asio::ip::address::from_string(nodes));
    cluster = builder->build();
    session = cluster->connect();
}

void SimpleClient::createSchema() 
{
    cout << "Creating simplex keyspace." << endl;
    shared_ptr<cql::cql_query_t> create_schema_statement(new cql::cql_query_t(
        string("CREATE KEYSPACE simplex WITH replication ") +
        "= {'class':'SimpleStrategy', 'replication_factor':3};"));
    boost::shared_future<cql::cql_future_result_t> future = session->query(create_schema_statement);
    future.wait();
    create_schema_statement.reset(new cql::cql_query_t(
        string("CREATE TABLE simplex.songs (") +
                  "id uuid PRIMARY KEY," + 
                  "title text," + 
                  "album text," + 
                  "artist text," + 
                  "tags set<text>," + 
                  "data blob" + 
              ");"
              ));
    future = session->query(create_schema_statement);
    future.wait();
    create_schema_statement.reset(new cql::cql_query_t(
        string("CREATE TABLE simplex.playlists (") +
                  "id uuid," +
                  "title text," +
                  "album text, " + 
                  "artist text," +
                  "song_id uuid," +
                  "PRIMARY KEY (id, title, album, artist)"
              ");"
              ));
    future = session->query(create_schema_statement);
    future.wait();
}

void SimpleClient::loadData() 
{
    cout << "Loading data into simplex keyspace." << endl;
    shared_ptr<cql::cql_query_t> load_data_statement(new cql::cql_query_t(
        string("INSERT INTO simplex.songs (id, title, album, artist, tags) ") +
            "VALUES (" +
                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                "'La Petite Tonkinoise'," +
                "'Bye Bye Blackbird'," +
                "'Joséphine Baker'," +
                "{'jazz', '2013'})" +
            ";"
            ));
    boost::shared_future<cql::cql_future_result_t> future = session->query(load_data_statement);
    future.wait();
    load_data_statement.reset(new cql::cql_query_t(
        string("INSERT INTO simplex.songs (id, title, album, artist, tags) ") +
            "VALUES (" +
                "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                "'Die Mösch'," +
                "'In Gold'," +
                "'Willi Ostermann'," +
                "{'kölsch', '1996', 'birds'}" +
            ");"
            ));
    future = session->query(load_data_statement);
    future.wait();
    load_data_statement.reset(new cql::cql_query_t(
        string("INSERT INTO simplex.songs (id, title, album, artist, tags) ") +
            "VALUES (" +
                "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                "'Memo From Turner'," +
                "'Performance'," +
                "'Mick Jager'," +
                "{'soundtrack', '1991'}" +
            ");"
            ));
    future = session->query(load_data_statement);
    future.wait();
    load_data_statement.reset(new cql::cql_query_t(
        string("INSERT INTO simplex.playlists (id, song_id, title, album, artist) ") +
            "VALUES (" +
                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                "'La Petite Tonkinoise'," +
                "'Bye Bye Blackbird'," +
                "'Joséphine Baker'" +
            ");"
            ));
    future.wait();
    future = session->query(load_data_statement);
    load_data_statement.reset(new cql::cql_query_t(
        string("INSERT INTO simplex.playlists (id, song_id, title, album, artist) ") +
            "VALUES (" +
                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                "'Die Mösch'," +
                "'In Gold'," +
                "'Willi Ostermann'" +
            ");"
            ));
    future.wait();
    future = session->query(load_data_statement);
    load_data_statement.reset(new cql::cql_query_t(
        string("INSERT INTO simplex.playlists (id, song_id, title, album, artist) ") +
            "VALUES (" +
                "3fd2bedf-a8c8-455a-a462-0cd3a4353c54," +
                "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                "'Memo From Turner'," +
                "'Performance'," +
                "'Mick Jager'" +
            ");"
            ));
}

void SimpleClient::querySchema()
{
    cout << "Querying the simplex.playlists table." << endl;
    shared_ptr<cql::cql_query_t> query_playlists_statement(new cql::cql_query_t(
        string("SELECT * FROM simplex.playlists ") +
            "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;"
            ));
    boost::shared_future<cql::cql_future_result_t> future = session->query(query_playlists_statement);
    std::cout << "Was the query successful? " << (!future.get().error.is_err() ? "Yes." : "No.") << endl;
    shared_ptr<cql::cql_result_t> result = future.get().result;
    cout << TITLE_COLUMN << "\t" << ALBUM_COLUMN << "\t" << ARTIST_COLUMN << endl;
    if ( result ) 
    {
        while ( result->next() ) 
        {
            string title, artist, album;
            result->get_string(TITLE_COLUMN, title);
            result->get_string(ALBUM_COLUMN, artist);
            result->get_string(ARTIST_COLUMN, album);
            cout << title << "\t" << album << "\t" << artist << endl;
        }
    }
}

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

void SimpleClient::dropSchema(const string schema) 
{
    cout << "Dropping " << schema << " keyspace." << "\n";
    shared_ptr<cql::cql_query_t> drop_schema_statement(new cql::cql_query_t(
        "DROP KEYSPACE " + schema
        ));
    session->query(drop_schema_statement).wait();
}

void SimpleClient::close() 
{
    cout << "Closing down cluster connection." << "\n";
    session->close();
    cluster->shutdown();
}


} // end namespace example




