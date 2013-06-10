using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraExamples
{
    public class SimpleCassandraClient
    {
        public SimpleCassandraClient()
        {
        }

        /*
        static void Main(string[] args)
        {
            SimpleClient client = new SimpleClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            Console.WriteLine("Hit any key to quit.");
            Console.ReadKey(false);
            client.DropSchema("simplex");
            client.Close();
        }
         * */
    }
}
