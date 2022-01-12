using System;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;
using MongoDbCRUD.Models;
using MongoDbCRUD.Repository;
using System.Threading;

namespace MongoDbCRUD
{
    internal class Program
    {
        public static string MainConnectionString = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0";
        public static string MainDatabaseName = "Anotacoes";
        static async Task Main(string[] args)
        {
            var repeatMenu = true;
            do 
            {
                Console.Clear();
                Console.WriteLine(BuildMenu());
                var optionMenu = Convert.ToInt32(Console.ReadLine());

                switch (optionMenu)
                {
                    case 1:
                        await InsertOneWithBsonDocument();
                        break;
                    case 2:
                        InsertOneWithOO();
                        break;
                    case 3:
                        await FindAll();
                        break;
                    default:
                        repeatMenu = false;
                        Console.WriteLine("Não escolheu uma opção válida");
                        break;
                }
            } while (repeatMenu);
            
        }        

        static string BuildMenu()
        {
            var menu = "1 - InsertOne - BsonDocument \n" +
                "2 - InsertOne - OO \n" +
                "3 - FindAll \n";

            return menu;
        }

        static IMongoDatabase GetDatabase(string connectionString, string dataBase)
        {
            var client = new MongoClient(connectionString);
            return client.GetDatabase(dataBase);
        }

        static async Task InsertOneWithBsonDocument()
        {
            Console.WriteLine("Digite sua anotação:");
            var anotacao = Console.ReadLine();
            var document = new BsonDocument();
            document.Add("Nota", anotacao);

            var db = GetDatabase(MainConnectionString, MainDatabaseName);
            var collection = db.GetCollection<BsonDocument>("NotasImportantes");

            
            await collection.InsertOneAsync(document);
            Console.WriteLine("Documento inserido");
        }

        static void InsertOneWithOO()
        {
            Console.WriteLine("Digite sua anotação:");
            var anotacao = Console.ReadLine();

            var notaImportante = new NotasImportantes()
            {
                Nota = anotacao
            };

            var notasImportantesRepository = new NotasImportantesRepository();
            notasImportantesRepository.Create(notaImportante);
            Console.WriteLine("Documento inserido");
        }

        static async Task FindAll()
        {
            var notasImportantesRepository = new NotasImportantesRepository();
            var listaAnotacoes = await notasImportantesRepository.GetAll();
            foreach(var item in listaAnotacoes)
            {
                Console.WriteLine(item.ToJson());
            }
            Console.ReadKey();
        }

        static void TransactionOperation()
        {
            var client = new MongoClient(MainConnectionString);

            // Prereq: Create collections.
            var database1 = client.GetDatabase("mydb-dont-exist");
            var collection1 = database1.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            collection1.InsertOne(new BsonDocument("Nota", "Primeira nota"));

            var database2 = client.GetDatabase("mydb-dont-exist-2");
            var collection2 = database2.GetCollection<BsonDocument>("Tarefas").WithWriteConcern(WriteConcern.WMajority);
            //collection2.InsertOne(new BsonDocument("Descricao", "Primeira tarefa"));

            // Step 1: Start a client session.
            using (var session = client.StartSession())
            {
                try
                {
                    // Step 3: Define the sequence of operations to perform inside the transactions
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            collection1.InsertOne(s, new BsonDocument("Nota", "Segunda nota"), cancellationToken: ct);
                            collection2.InsertOne(s, new BsonDocument("Descricao", "Terceira nota"), cancellationToken: ct);
                            return "Inserted into collections in different databases";
                        }, null, CancellationToken.None);
                } 
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                    Console.ReadKey();
                }                
            }
        }
    }
}
