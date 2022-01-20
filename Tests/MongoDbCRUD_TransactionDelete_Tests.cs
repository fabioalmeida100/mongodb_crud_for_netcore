using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.TryDeleteUsingTransaction
{
    public class MongoDbCRUD_TransactionDelete_Tests: TestFixture
    {
        [Fact]
        public async Task TryDeleteOneAsync_OneDocument_DeleteDocument()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";            

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, primeiraAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, segundaAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, terceiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    
                    var filter = Builders<BsonDocument>.Filter.Eq(campo, segundaAnotacao);

                    await anotacoesCollection.DeleteOneAsync(session, filter);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument()).ToList();
            anotacoes.Should().NotBeNull();
            anotacoes.Count.Should().Be(2);

            anotacoesCollection.Find(new BsonDocument(campo, segundaAnotacao)).CountDocuments().Should().Be(0);

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryDeleteManyAsync_VariousDocuments_DeleteDocuments()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var quartaAnotacao = "Quarta nota";
            var quintaAnotacao = "Quinta nota";            

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, primeiraAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, segundaAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, terceiraAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, quartaAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, quintaAnotacao));            
            
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    var filter = Builders<BsonDocument>.Filter.Empty;
                    await anotacoesCollection.DeleteManyAsync(session, filter);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            anotacoesCollection.Find(new BsonDocument()).CountDocuments().Should().Be(0);
            exception.Should().BeNull();
        }
    }
}
