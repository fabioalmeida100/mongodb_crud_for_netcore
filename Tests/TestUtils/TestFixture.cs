using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.TestUtils
{
    public class TestFixture
    {
        protected string ConnectionString { 
            get 
            {
                return "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0";
            }
        }

        protected IMongoClient Client;

        protected IMongoDatabase Database;

        public TestFixture()
        {
            var dataBaseName = "DbTest";
            Client = new MongoClient(ConnectionString);
            Client.DropDatabase(dataBaseName);

            Database = Client.GetDatabase(dataBaseName);
        }
    }
}
