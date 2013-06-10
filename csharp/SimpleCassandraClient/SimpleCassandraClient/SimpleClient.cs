using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraExamples
{
    public class SimpleClient
    {
        public Session session { get; private set; }

        public SimpleClient()
        {
        }

        public void Connect(String node)
        {
            Cluster cluster = Cluster.Builder()
                   .AddContactPoint(node)
                   .Build();
            session = cluster.Connect();
            Metadata metadata = cluster.Metadata;
            Console.WriteLine("Connected to cluster: " + metadata.GetClusterName().ToString());
        }

        public void CreateSchema()
        {
            session.Execute(
                "CREATE KEYSPACE simplex WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':1};");
            // create songs and playlist tables
            session.Execute(
                "CREATE TABLE simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                    ");");
            session.Execute(
                "CREATE TABLE simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
            Console.WriteLine("Simplex keyspace and schema created.");
        }

        public void LoadData()
        {
            // insert data in the tables
            session.Execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                    "'La Petite Tonkinoise'," +
                    "'Bye Bye Blackbird'," +
                    "'Joséphine Baker'," +
                    "{'jazz', '2013'})" +
                ";");
            session.Execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                    "'Die Mösch'," +
                    "'In Gold'," +
                    "'Willi Ostermann'," +
                    "{'kölsch', '1996', 'birds'}" +
                ");");
            session.Execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                "VALUES (" +
                    "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                    "'Memo From Turner'," +
                    "'Performance'," +
                    "'Mick Jager'," +
                    "{'soundtrack', '1991'}" +
                ");");
            session.Execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                    "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                    "'La Petite Tonkinoise'," +
                    "'Bye Bye Blackbird'," +
                    "'Joséphine Baker'" +
                ");");
            session.Execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                    "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                    "'Die Mösch'," +
                    "'In Gold'," +
                    "'Willi Ostermann'" +
                ");");
            session.Execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "3fd2bedf-a8c8-455a-a462-0cd3a4353c54," +
                    "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                    "'Memo From Turner'," +
                    "'Performance'," +
                    "'Mick Jager'" +
                ");");
            Console.WriteLine("Data loaded.");
        }

        public void QuerySchema()
        {
            session.Execute(
                "UPDATE simplex.songs " +
                "SET tags = tags + { 'entre-deux-guerre' } " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

            CqlRowSet results = session.Execute(
                "SELECT * FROM simplex.songs " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

            Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3, -30}",
                "title", "album", "artist", "tags"));
            Console.WriteLine("-------------------------------+-----------------------+--------------------+-------------------------------");
            foreach (CqlRow row in results.GetRows())
            {
                Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3}",
                    row.GetValue<String>("title"), row.GetValue<String>("album"),
                    row.GetValue<String>("artist"), row.GetValue<List<String>>("tags").ToString()));
            }
        }

        public void UpdateSchema()
        {
            session.Execute(
                "UPDATE simplex.songs " +
                "SET tags = tags + { 'entre-deux-guerre' } " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

            CqlRowSet results = session.Execute(
                "SELECT * FROM simplex.songs " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

            Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}", "title", "album", "artist", "tags"));
            Console.WriteLine("-------------------------------+-----------------------+--------------------+-------------------------------");
            foreach (CqlRow row in results.GetRows())
            {
                Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}",
                    row.GetValue<String>("title"), row.GetValue<String>("album"),
                    row.GetValue<String>("artist"), row.GetValue<List<String>>("tags")));
            }
        }

        public void DropSchema(String keyspace)
        {
            session.Execute("DROP KEYSPACE " + keyspace + ";");
        }

        public void Close()
        {
            session.Cluster.Shutdown();
            session.Dispose();
        }

        /*
        static void Main(string[] args)
        {
            SimpleCassandraClient client = new SimpleCassandraClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            Console.WriteLine("Hit any key to quit.");
            Console.ReadKey(false);
            client.DropSchema("simplex");
            client.Close();
        }
         */
    }
}
