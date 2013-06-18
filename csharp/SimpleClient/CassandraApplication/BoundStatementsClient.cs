using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class BoundStatementsClient : SimpleClient
    {
        public BoundStatementsClient()
        {
        }

        public override void LoadData()
        {
            PreparedStatement statement = Session.Prepare(
                  "INSERT INTO simplex.songs " +
                  "(id, title, album, artist, tags) " +
                  "VALUES (?, ?, ?, ?, ?);");
            BoundStatement boundStatement = new BoundStatement(statement);
            HashSet<String> tags = new HashSet<String>();
            tags.Add("jazz");
            tags.Add("2013");
            Session.Execute(boundStatement.Bind(
                  new Guid("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                  "La Petite Tonkinoise'",
                  "Bye Bye Blackbird'",
                  "Joséphine Baker",
                  tags)
                );
            tags = new HashSet<String>();
            tags.Add("1996");
            tags.Add("nirds");
            Session.Execute(boundStatement.Bind(
                  new Guid("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
                  "Die Mösch",
                  "In Gold'",
                  "Willi Ostermann",
                  tags)
                );
            tags = new HashSet<String>();
            tags.Add("1970");
            tags.Add("soundtrack");
            Session.Execute(boundStatement.Bind(
                  new Guid("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
                  "Memo From Turner",
                  "Performance",
                  "Mick Jager",
                  tags)
                );
            // playlists table
            statement = Session.Prepare(
                  "INSERT INTO simplex.playlists " +
                  "(id, song_id, title, album, artist) " +
                  "VALUES (?, ?, ?, ?, ?);");
            boundStatement = new BoundStatement(statement);
            Session.Execute(boundStatement.Bind(
                  new Guid("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                  new Guid("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                  "La Petite Tonkinoise",
                  "Bye Bye Blackbird",
                  "Joséphine Baker"));
            Session.Execute(boundStatement.Bind(
                  new Guid("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                  new Guid("f6071e72-48ec-4fcb-bf3e-379c8a696488"),
                  "Die Mösch",
                  "In Gold",
                  "Willi Ostermann"));
            Session.Execute(boundStatement.Bind(
                  new Guid("3fd2bedf-a8c8-455a-a462-0cd3a4353c54"),
                  new Guid("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25"),
                  "Memo From Turner",
                  "Performance",
                  "Mick Jager"));
        }

    }
}
