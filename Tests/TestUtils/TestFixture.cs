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
        protected string ConnectionStringReplicaSet { 
            get 
            {
                return "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0";
            }
        }

        protected string ConnectionStringStandalone
        {
            get
            {
                return "mongodb://localhost:30000";
            }
        }

        protected IMongoClient ClientReplicaSet;        
        protected IMongoDatabase DatabaseReplicasetDbTest;

        protected IMongoClient ClientStandalone;
        protected IMongoDatabase DatabaseStandalone;

        public TestFixture()
        {
            var dataBaseName = "DbTest";

            ClientReplicaSet = new MongoClient(ConnectionStringReplicaSet);
            ClientReplicaSet.DropDatabase(dataBaseName);
            DatabaseReplicasetDbTest = ClientReplicaSet.GetDatabase(dataBaseName);

            ClientStandalone = new MongoClient(ConnectionStringStandalone);
            ClientStandalone.DropDatabase(dataBaseName);
            DatabaseStandalone = ClientStandalone.GetDatabase(dataBaseName);
        }
    }
}
