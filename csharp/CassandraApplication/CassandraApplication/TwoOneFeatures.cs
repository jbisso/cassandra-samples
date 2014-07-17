using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace CassandraApplication
{
    class TwoOneFeatures : SimpleClient
    {
        private static String INSERT_USER = "INSERT INTO users (id, name, addresses) " +
            "VALUES (:id, :name, :addresses);";
        private PreparedStatement InsertUserPreparedStatement;

        public TwoOneFeatures() { }

        public void PrepareStatements()
        {
            InsertUserPreparedStatement = Session.Prepare(INSERT_USER);
        }

        public override void CreateSchema()
        {
            Console.WriteLine("Creating complex schema.");
            Dictionary<String, String> replication = new Dictionary<String, String>();
            replication.Add("class", "SimpleStrategy");
            replication.Add("replication_factor", "3");
            Session.CreateKeyspace("complex", replication);
            Session.ChangeKeyspace("complex");
            // create the UDT
            Session.Execute(
                "CREATE TYPE address (" +
                    "street text," +
                    "city text," +
                    "zip_code int," +
                    "phones list<text>);"
                );
            // create a table that uses it
            Session.Execute(
                "CREATE TABLE users (" +
                    "id uuid PRIMARY KEY," +
                    "name text," +
                    "addresses map<text, address>);"
                );
            // map the UDT to the class
            Session.UserDefinedTypes.Define(
                UdtMap.For<Address>()
                .Map(a => a.ZipCode, "zip_code")
                );
        }

        public override void LoadData()
        {
            Console.WriteLine("Loading data into schema.");
            Address address = new Address();
            address.Street = "123 Arnold Drive";
            address.City = "Sonoma";
            address.ZipCode = 95476;
            List<String> phones = new List<String>();
            phones.Add("707-555-1234");
            phones.Add("800-555-9876");
            address.Phones = phones;
            Dictionary<String, Address> addressesMap = new Dictionary<String, Address>();
            addressesMap.Add("Home", address);
            BoundStatement boundStatement = InsertUserPreparedStatement.Bind(
                new
                {
                    id = new Guid("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                    name = "John Doe",
                    addresses = addressesMap
                }
                );
            Session.Execute(boundStatement);
        }

        public override void QuerySchema()
        {
            Console.WriteLine("Querying data.");
            RowSet results = Session.Execute("SELECT * FROM users " +
                "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
            foreach (var row in results)
            {
                IDictionary<String, Address> addresses = row.GetValue<IDictionary<String, Address>>("addresses");
                Console.WriteLine(row.GetValue<String>("name"));
                foreach (var entry in addresses)
                {
                    Console.WriteLine(entry.Key + " address: " + entry.Value);
                }
            }
        }
    }

    public class Address
    {
        public String Street { get; set; }
        public String City { get; set; }
        public int ZipCode { get; set; }
        public IEnumerable<String> Phones { get; set; }
        public override String ToString()
        {
            return Street + ", " + City + ", " + ZipCode;
        }
    }
}
