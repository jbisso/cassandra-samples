using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraExamples
{
    public class AsynchronousExamples : SimpleClient
    {
        public AsynchronousExamples()
        {
        }

        public CqlRowSet getRows()
        {
            //Query query = QueryBuilder.select().all().from("simplex", "songs");
            //return session.executeAsync(query);
            Query query = new SimpleStatement("SELECT * FROM simplex.songs;");

            return session.Execute(query);
        }
    }
}
