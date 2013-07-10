using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;

namespace CassandraApplication
{
    class EmbeddedCqlClient
    {
        String[] _names = { "Jim", "Sirje", "Erling" };

        public String[] Names { get { return _names; } }

        public EmbeddedCqlClient()
        {
        }

        public void doArrayQuery()
        {
            IEnumerable<String> query = from s in _names
                                        where s.Length == 5
                                        orderby s
                                        select s.ToUpper();

            foreach (String item in query)
            {
                Console.WriteLine(item);
            }
        }

        public void doCqlQuery()
        {
            var cluster = Cluster.Builder()
                .AddContactPoint("localhost")
                .Build();

            var session = cluster.Connect("simplex");

            var context = new Context(session);

            /*
            var table = context.GetTable<TEnt>();

            foreach (var ent in (from e in table select e).Execute())
            {
                // do something ...
            }
             * */
        }
    }
}
