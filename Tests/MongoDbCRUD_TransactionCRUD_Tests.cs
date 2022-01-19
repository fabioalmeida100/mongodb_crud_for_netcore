using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Tests.TryCRUDUsingTransaction
{
    public class MongoDbCRUD_TransactionCRUD_Tests: TestFixture
    {
        [Fact]
        public async Task TryCRUDOperation_WhenInsertOneAsyncMultipleDocumentsAndUpdateAndDeleteSomeDocuments_ExecuteAllOperations()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var quartaAnotacao = "Quarta nota";
            var segundaAnotacaoAtualizada = "Segunda nota - atualizada";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, primeiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    // Insert 3 documents
                    
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, segundaAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, terceiraAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, quartaAnotacao));

                    // Update 1 document
                    var filterUpdate = Builders<BsonDocument>.Filter.Eq(campo, segundaAnotacao);
                    var update = Builders<BsonDocument>.Update.Set(campo, segundaAnotacaoAtualizada);
                    await anotacoesCollection.UpdateOneAsync(session, filterUpdate, update);

                    // Delete one
                    var filterDelete = Builders<BsonDocument>.Filter.Eq(campo, primeiraAnotacao);
                    await anotacoesCollection.DeleteOneAsync(session, filterDelete);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    await session.AbortTransactionAsync();
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument()).ToList();
            anotacoes.Should().NotBeNull();
            anotacoes.Count.Should().Be(3);

            var segunda = anotacoesCollection.Find(new BsonDocument(campo, segundaAnotacaoAtualizada));
            segunda.CountDocuments().Should().Be(1);
            segunda.FirstOrDefault().GetValue(campo).Should().Be(segundaAnotacaoAtualizada);
            
            anotacoesCollection.Find(new BsonDocument(campo, primeiraAnotacao)).CountDocuments().Should().Be(0);

            exception.Should().BeNull();
        }

        public async Task TryCRUDOperation_WhenUpdateManyAsyncSomeDocuments_ExecuteAllOperations()
        {
            // Arrange
            var campo = "Nota";
            var anotacaoRepetida = "Anotação Repetida";
            var anotacaoAtualizada = "Anotações atualizada";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, anotacaoRepetida));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    // Insert 2 documents
                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument(campo, anotacaoRepetida));
                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument(campo, anotacaoRepetida));

                    // Update 1 document
                    var filterUpdate = Builders<BsonDocument>.Filter.Eq(campo, anotacaoRepetida);
                    var update = Builders<BsonDocument>.Update.Set(campo, anotacaoAtualizada);
                    await anotacoesCollection.UpdateManyAsync(session, filterUpdate, update);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    await session.AbortTransactionAsync();
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument()).ToList();
            anotacoes.Should().NotBeNull();
            anotacoes.Count.Should().Be(3);
            anotacoes.ForEach(x => x.GetValue(campo).Should().Be(anotacaoAtualizada));

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryCRUDOperation_WhenDeleteDocumentOutsideTransaction_WriteConflict()
        {
            // Arrange
            var campo = "Nota";
            var anotacaoRepetida = "Anotação Repetida";
            var anotacaoAtualizada = "Anotações atualizada";
            var anotacoesListaDesatualizada = new List<BsonDocument>();

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, anotacaoRepetida));
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, anotacaoRepetida));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    anotacoesListaDesatualizada = anotacoesCollection.Find(session, new BsonDocument()).ToList();

                    // Delete documenta outside transation (dont use session)
                    var filter = Builders<BsonDocument>.Filter.Eq(campo, anotacaoRepetida);
                    await anotacoesCollection.DeleteManyAsync(filter);

                    // Try update deleted documents
                    filter = Builders<BsonDocument>.Filter.Eq(campo, anotacaoRepetida);
                    var update = Builders<BsonDocument>.Update.Set(campo, anotacaoAtualizada);
                    await anotacoesCollection.UpdateManyAsync(session, filter, update);

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    await session.AbortTransactionAsync();
                    exception = ex;
                }
            }

            //Assert
            anotacoesCollection.Find(new BsonDocument(campo, anotacaoAtualizada)).CountDocuments().Should().Be(0);
            anotacoesListaDesatualizada.ForEach(item => { item.GetValue(campo).Should().Be(anotacaoRepetida); });
            exception.Should().NotBeNull();
        }
    }
}
