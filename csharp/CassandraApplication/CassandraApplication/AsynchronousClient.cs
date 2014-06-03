using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class AsynchronousClient : SimpleClient
    {
        public AsynchronousClient() { }

        public override void QuerySchema()
        {
            Statement statement = new SimpleStatement("SELECT * FROM simplex.songs;");
            var task = Session.ExecuteAsync(statement);
            task.ContinueWith((asyncTask) =>
            {
                Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3, -30}",
                    "title", "album", "artist", "tags"));
                Console.WriteLine("-------------------------------+-----------------------+--------------------+-------------------------------");
                foreach (var row in asyncTask.Result)
                {
                    Console.WriteLine(String.Format("{0, -30}\t{1, -20}\t{2, -20}\t{3}",
                        row.GetValue<String>("title"), row.GetValue<String>("album"),
                        row.GetValue<String>("artist"), Prettify(row.GetValue<List<String>>("tags")
                        )));
                }
            });
            task.Wait();
        }


        internal String Prettify(IEnumerable<String> collection)
        {
            StringBuilder result = new StringBuilder("[ ");
            foreach (var item in collection)
            {
                result.Append(item);
                result.Append(" ");
            }
            result.Append("]");
            return result.ToString();
        }
    }
}
