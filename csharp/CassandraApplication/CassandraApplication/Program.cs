using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            //SimpleClient client = new SimpleClient();
            //BoundStatementsClient client = new BoundStatementsClient();
            AsynchronousClient client = new AsynchronousClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            Console.ReadKey();
            client.DropSchema("simplex");
            client.Close();
            client.Close();
        }
    }
}
