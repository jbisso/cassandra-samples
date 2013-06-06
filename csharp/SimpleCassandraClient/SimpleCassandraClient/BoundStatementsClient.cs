using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraExamples
{
    public class BoundStatementsClient : SimpleCassandraClient
    {
        public BoundStatementsClient()
        {
        }

        public void LoadData()
        {
            PreparedStatement statement = getSession().prepare(
                  "INSERT INTO songs " +
                  "(id, title, album, artist, tags) " +
                  "VALUES (?, ?, ?, ?, ?);");
            BoundStatement boundStatement = new BoundStatement(statement);
            Set<String> tags = new HashSet<String>();
            tags.add("jazz");
            tags.add("2013");
            getSession().execute(boundStatement.bind(
                  UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                  "La Petite Tonkinoise'",
                  "Bye Bye Blackbird'",
                  "Joséphine Baker",
                  tags));
            tags = new HashSet<String>();
            tags.add("1996");
            tags.add("1996");
            tags.add("birds");
            getSession().execute(boundStatement.bind(
                  UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
                  "Die Mösch",
                  "In Gold'",
                  "Willi Ostermann",
                  tags));
            tags = new HashSet<String>();
            tags.add("1996");
            tags.add("1996");
            tags.add("birds");
            getSession().execute(boundStatement.bind(
                  UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
                  "Memo From Turner",
                  "Performance",
                  "Mick Jager",
                  tags));
            // playlists table
            statement = getSession().prepare(
                  "INSERT INTO playlists " +
                  "(id, song_id, title, album, artist) " +
                  "VALUES (?, ?, ?, ?, ?);");
            boundStatement = new BoundStatement(statement);
            getSession().execute(boundStatement.bind(
                  UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                  UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                  "La Petite Tonkinoise",
                  "Bye Bye Blackbird",
                  "Joséphine Baker"));
            getSession().execute(boundStatement.bind(
                  UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                  UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
                  "Die Mösch",
                  "In Gold",
                  "Willi Ostermann"));
            getSession().execute(boundStatement.bind(
                  UUID.fromString("3fd2bedf-a8c8-455a-a462-0cd3a4353c54"),
                  UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
                  "Memo From Turner",
                  "Performance",
                  "Mick Jager"));
        }

        static void Main(string[] args)
        {
            BoundStatementsClient client = new BoundStatementsClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            client.UpdateSchema();
            client.DropSchema("simplex");
            client.Close();
        }
    }
}
