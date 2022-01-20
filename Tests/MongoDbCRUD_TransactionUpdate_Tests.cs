using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.TryUpdateUsingTransaction
{
    public class MongoDbCRUD_TransactionUpdate_Tests: TestFixture
    {
        [Fact]
        public async Task TryUpdateOneAsync_OneDocument_UpdateDocument()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var segundaAnotacaoAtualizada = "Segunda nota - atualizada";

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
                    var update = Builders<BsonDocument>.Update.Set(campo, segundaAnotacaoAtualizada);

                    await anotacoesCollection.UpdateOneAsync(session, filter, update);

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
            anotacoes.Count.Should().Be(3);
                        
            anotacoesCollection.Find(new BsonDocument(campo, segundaAnotacaoAtualizada)).CountDocuments().Should().Be(1);

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryUpdateManyAsync_VariousDocuments_UpdateDocuments()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var quartaAnotacao = "Quarta nota";
            var quintaAnotacao = "Quinta nota";
            var notaRepetida = "Nota repetida";
            var notaRepetidaAtualizada = "Nota repetida - atualizada";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, primeiraAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, segundaAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, terceiraAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, quartaAnotacao));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, quintaAnotacao));

            anotacoesCollection.InsertOne(new BsonDocument(campo, notaRepetida));
            anotacoesCollection.InsertOne(new BsonDocument(campo, notaRepetida));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    
                    var filter = Builders<BsonDocument>.Filter.Eq(campo, notaRepetida);
                    var update = Builders<BsonDocument>.Update.Set(campo, notaRepetidaAtualizada);
                    await anotacoesCollection.UpdateManyAsync(session, filter, update);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            anotacoesCollection.Find(new BsonDocument(campo, notaRepetidaAtualizada)).CountDocuments().Should().Be(2);
            exception.Should().BeNull();
        }
    }
}
