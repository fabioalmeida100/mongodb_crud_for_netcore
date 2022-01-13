using System;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;
using MongoDbCRUD.Models;
using MongoDbCRUD.Repository;

namespace MongoDbCRUD
{
    internal class Program
    {
        public static string MainConnectionString = "mongodb://localhost:27017";
        public static string MainDatabaseName = "Anotacoes";
        static async Task Main(string[] args)
        {
            var repeatMenu = true;
            do 
            {
                Console.Clear();
                Console.WriteLine(BuildMenu());
                int.TryParse(Console.ReadLine(), out var optionMenu);

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
                    case 4:
                        await FindOne();
                        break;
                    case 5:
                        await UpdateOne();
                        break;
                    case 6:
                        await DeleteOne();
                        break;
                    case 7:
                        Console.WriteLine("Até a próxima :-)");
                        repeatMenu = false;
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
                "3 - FindAll \n" +
                "4 - FindOne \n" +
                "5 - UpdateOne \n" +
                "6 - DeleteOne \n" + 
                "7 - Sair";

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

        public static async Task FindOne()
        {
            var notasImportantesRepository = new NotasImportantesRepository();
            Console.WriteLine("Digite o objectId:");
            var id = Console.ReadLine();
            var anotacao = new NotasImportantes();
            try
            {
                anotacao = await notasImportantesRepository.GetById(id);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Descrição da anotação {0}: ", anotacao.Nota);
            Console.WriteLine();
            Console.ReadKey();
        }

        public static async Task UpdateOne()
        {
            var notasImportantesRepository = new NotasImportantesRepository();
            Console.WriteLine("Digite o objectId:");
            var id = Console.ReadLine();            
            try
            {
                var anotacao = await notasImportantesRepository.GetById(id);
                if (anotacao == null)
                {
                    Console.WriteLine("Anotação não encontrada.");
                }
                else
                {
                    Console.WriteLine("Digite a nova descrição:");                    
                    anotacao.Nota = Console.ReadLine();
                    notasImportantesRepository.UpdateById(anotacao);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task DeleteOne()
        {
            var notasImportantesRepository = new NotasImportantesRepository();
            Console.WriteLine("Digite o objectId:");
            var id = Console.ReadLine();
            try
            {
                var anotacao = await notasImportantesRepository.GetById(id);
                if (anotacao == null)
                {
                    Console.WriteLine("Anotação não encontrada.");
                }
                else
                {
                    notasImportantesRepository.DeleteById(id);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
