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
            //AsynchronousClient client = new AsynchronousClient();
            TwoOneFeatures client = new TwoOneFeatures();
            //client.Connect("127.0.0.1");
            client.Connect("ec2-54-176-125-19.us-west-1.compute.amazonaws.com");
            client.CreateSchema();
            client.PrepareStatements();
            client.LoadData();
            client.QuerySchema();
            Console.WriteLine("Hit return to quit.");
            Console.ReadKey();
            //client.DropSchema("simplex");
            client.DropSchema("complex");
            client.Close();
        }
    }
}
