using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;
using System.Linq;

namespace Tests.TryInsertOne
{
    public class MongoDbCRUD_TransactionInsertOne_Tests: TestFixture
    {
        [Fact]
        public void TryInsertOne_WhenDatabaseExist_InsertDocuments()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            anotacoesCollection.InsertOne(new BsonDocument(campo, primeiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = ClientReplicaSet.StartSession())
            {
                try
                {
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            anotacoesCollection.InsertOne(s, new BsonDocument(campo, segundaAnotacao), cancellationToken: ct);
                            return "Inserted into collections in different databases";
                        }, null, CancellationToken.None);
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
                        
            anotacoesCollection.Find(new BsonDocument(campo, primeiraAnotacao)).CountDocuments().Should().Be(1);
            anotacoesCollection.Find(new BsonDocument(campo, segundaAnotacao)).CountDocuments().Should().Be(1);

            exception.Should().BeNull();
        }

        [Fact]
        public void TryInsertOne_WhenDatabaseServerIsStandalone_ReturnException()
        {
            // Arrange
            var anotacoesCollection = DatabaseStandalone.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            anotacoesCollection.InsertOne(new BsonDocument("Nota", "Primeira nota"));
            Exception exception = null;

            // Act
            using (var session = ClientStandalone.StartSession())
            {
                try
                {
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            anotacoesCollection.InsertOne(s, new BsonDocument("Nota", "Segunda nota"), cancellationToken: ct);
                            return "Inserted into collections in different databases";
                        }, null, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            exception.Should().NotBeNull();
        }

        [Fact]
        public void TryInsertOne_WhenDatabaseDontExist_ReturnException()
        {
            // Arrange
            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            Exception exception = null;

            // Act
            using (var session = ClientReplicaSet.StartSession())
            {
                try
                {
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            anotacoesCollection.InsertOne(s, new BsonDocument("Nota", "Segunda nota"), cancellationToken: ct);
                            return "Inserted into collections in different databases";
                        }, null, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            exception.Should().NotBeNull();
        }

        [Fact]
        public void TryInsertOneMultiplesDocuments_WhenDatabaseExist_InsertDocuments()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var quartaAnotacao = "Quarta nota";
            var quintaAnotacao = "Quinta nota";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            anotacoesCollection.InsertOne(new BsonDocument(campo, primeiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = ClientReplicaSet.StartSession())
            {
                try
                {
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            anotacoesCollection.InsertOne(s, new BsonDocument(campo, segundaAnotacao), cancellationToken: ct);
                            anotacoesCollection.InsertOne(s, new BsonDocument(campo, terceiraAnotacao), cancellationToken: ct);
                            anotacoesCollection.InsertOne(s, new BsonDocument(campo, quartaAnotacao), cancellationToken: ct);
                            anotacoesCollection.InsertOne(s, new BsonDocument(campo, quintaAnotacao), cancellationToken: ct);
                            return "Inserted into collections in different databases";
                        }, null, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument()).ToList();
            anotacoes.Should().NotBeNull();
            anotacoes.Count.Should().Be(5);

            exception.Should().BeNull();
        }
    }
}
