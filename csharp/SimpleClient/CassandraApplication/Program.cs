using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
            SimpleClient client = new SimpleClient();
            BoundStatementsClient client = new BoundStatementsClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            Pause();
            client.DropSchema("simplex");
            client.Close();
             */
            AsynchronousClient client = new AsynchronousClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();

            IAsyncResult asyncResult = client.GetRowsAsynchronously("SELECT * FROM simplex.songs;");
            asyncResult.AsyncWaitHandle.WaitOne();
            CqlRowSet results = client.Session.EndExecute(asyncResult);
            client.PrintResults(results);

            Pause();
            client.DropSchema("simplex");
            client.Close();
        }

        public static void Pause()
        {
            Console.WriteLine("Hit any key to quit.");
            Console.ReadKey(true);
        }
    }
}
