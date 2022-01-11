using System;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;
using MongoDbCRUD.Models;

namespace MongoDbCRUD
{
    internal class Program
    {
        private static string _connectionString = "mongodb://localhost:27017";
        private static string _databaseName = "Mensagens";
        static void Main(string[] args)
        {
            Console.WriteLine(BuildMenu());
            var optionMenu = Convert.ToInt32(Console.ReadLine());

            switch (optionMenu)
            {
                case 1:
                    InsertOne();
                    break;
                default: 
                    Console.WriteLine("Não escolheu uma opção válida");
                    break;
            }
        }

        static string BuildMenu()
        {
            var menu = "1 - InsertOne \n" +
                "2 - UpdateOne \n" +
                "3 - FindAll \n" +
                "4 - FindOne \n" +
                "5 - DeleteOne \n" +
                "6 - DeleteMany";

            return menu;
        }

        static IMongoDatabase GetDatabase(string connectionString, string dataBase)
        {
            var client = new MongoClient(connectionString);
            return client.GetDatabase(dataBase);
        }

        static async Task InsertOne()
        {
            var document = new BsonDocument();
            Console.WriteLine("Digite sua anotação:");
            var anotacao = Console.ReadLine();
            document.Add("Nota", string.Format("Minha nota: {0}", anotacao));

            var db = GetDatabase(_connectionString, _databaseName);
            var collection = db.GetCollection<BsonDocument>("Anotacoes");
            await collection.InsertOneAsync(document);
        }
    }
}
