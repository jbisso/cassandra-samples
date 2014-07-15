#include "SimpleClient.hpp"
#include "BoundStatementsClient.hpp"

#include <cql/cql_execute.hpp>

namespace example
{

const std::string TAGS_COLUMN("tags");
const std::string TITLE_COLUMN("title");
const std::string ARTIST_COLUMN("artist");
const std::string ALBUM_COLUMN("album");

using namespace std;
using namespace cql;
using boost::shared_ptr;

void prepareStatements()
{
}

void BoundStatementsClient::loadData()
{
    "INSERT INTO songs (id, title, album, artist) VALUES (?, ?, ?, ?);");
    
    // compile the parametrized query on the server
    boost::shared_future<cql::cql_future_result_t> future = getSession()->prepare(unbound_select);
    future.wait();
    // read the hash (ID) returned by Cassandra as identificator of prepared query
    std::vector<cql::cql_byte_t> queryId = future.get().result->query_id();
    boost::shared_ptr<cql::cql_execute_t> bound(
        new cql::cql_execute_t(queryId, cql::CQL_CONSISTENCY_ONE));
    // bind the query with concrete parameter: "system_auth"
    bound->push_back("756716f7-2e54-4715-9f00-91dcbea6cf50");
    bound->push_back("La Petite Tonkinoise'");
    bound->push_back("Bye Bye Blackbird'");
    bound->push_back("Joséphine Baker");
    // send the concrete (bound) query
    future = getSession()->execute(bound);
    future.wait();
    cout << "Querying the simplex.playlists table." << endl;
    shared_ptr<cql::cql_query_t> query_playlists_statement(new cql::cql_query_t(
        string("SELECT * FROM simplex.songs;")
        ));
    future = getSession()->query(query_playlists_statement);
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

BoundStatementsClient::BoundStatementsClient() { }

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
            "Jos�phine Baker",
            tags ) );

*/

