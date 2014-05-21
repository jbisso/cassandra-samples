using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class SimpleClient
    {
        public Cluster Cluster { get; private set; }
        public ISession Session { get; private set; }

        public SimpleClient() { }

        public void Connect(String node)
        {
            Cluster = Cluster.Builder()
             .AddContactPoint(node)
             .Build();
            Console.WriteLine("Connected to cluster: " + Cluster.Metadata.ClusterName.ToString());
            foreach (var host in Cluster.Metadata.AllHosts())
            {
                Console.WriteLine("Data Center: " + host.Datacenter + ", " +
                    "Host: " + host.Address + ", " +
                    "Rack: " + host.Rack);
            }
            Session = Cluster.Connect();
        }

        public void CreateSchema()
        {
            Session.Execute("CREATE KEYSPACE simplex WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor' : 3};");
            Session.Execute(
                "CREATE TABLE simplex.songs (" +
                    "id uuid PRIMARY KEY," +
                    "title text," +
                    "album text," +
                    "artist text," +
                    "tags set<text>," +
                    "data blob" +
                ");");
            Session.Execute(
                  "CREATE TABLE simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
        }

        public virtual void LoadData()
        {
             Session.Execute(
                  "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                  "VALUES (" +
                      "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                      "'La Petite Tonkinoise'," +
                      "'Bye Bye Blackbird'," +
                      "'Joséphine Baker'," +
                      "{'jazz', '2013'})" +
                      ";");
            Session.Execute(
                  "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                  "VALUES (" +
                      "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                      "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                      "'La Petite Tonkinoise'," +
                      "'Bye Bye Blackbird'," +
                      "'Joséphine Baker'" +
                      ");");
        }

        public virtual void QuerySchema()
        {
            RowSet results = Session.Execute("SELECT * FROM simplex.playlists " +
                "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
            Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}",
                    "title", "album", "artist"));
            Console.WriteLine("-------------------------------+-----------------------+--------------------+-------------------------------");
            foreach (var row in results)
            {
                Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}",
                    row.GetValue<String>("title"), row.GetValue<String>("album"),
                    row.GetValue<String>("artist")));
            }
        }

        public void DropSchema(String keyspace)
        {
            Session.Execute("DROP KEYSPACE " + keyspace);
            Console.WriteLine("Finished dropping " + keyspace + " keyspace.");
        }

        public void Close()
        {
            Cluster.Shutdown();
            Session.Dispose();
        }
    }
}
