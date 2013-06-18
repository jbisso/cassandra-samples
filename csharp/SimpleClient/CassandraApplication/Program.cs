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
            BoundStatementsClient client = new BoundStatementsClient();
            client.Connect("127.0.0.1");
            client.CreateSchema();
            client.LoadData();
            client.QuerySchema();
            Console.WriteLine("Hit any key to quit.");
            Console.ReadKey(true);
            client.DropSchema("simplex");
            client.Close();
        }
    }
}
