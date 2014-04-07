using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class AsynchronousClient : SimpleClient
    {
        public AsynchronousClient() { }

        public RowSet GetRows()
        {
            Statement query = new SimpleStatement("SELECT * FROM simplex.songs;");
            IAsyncResult asyncResult = Session.BeginExecute(query, null, null);
            asyncResult.AsyncWaitHandle.WaitOne();
            RowSet result = Session.EndExecute(asyncResult);
            return result;
        }

        public IAsyncResult GetRowsAsynchronously(String query)
        {
            Statement statement = new SimpleStatement(query);
            return Session.BeginExecute(statement, null, null);
        }

        public void PrintResults(RowSet results)
        {
            Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3, -30}",
                "title", "album", "artist", "tags"));
            Console.WriteLine("-------------------------------+-----------------------+--------------------+-------------------------------");
            foreach (Row row in results.GetRows())
            {
                Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3}",
                    row.GetValue<String>("title"), row.GetValue<String>("album"),
                    row.GetValue<String>("artist"), Prettify( row.GetValue<List<String>>("tags")
                    ) ));
            }
        }
    }
}
