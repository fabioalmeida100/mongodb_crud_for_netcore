using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tests.TryInsertUsingTransaction
{
    public class MongoDbCRUD_TransactionInsert_Tests: TestFixture
    {
        #region Inserts using sync methods
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
                    var result = session.WithTransaction((s, ct) =>
                    {
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, segundaAnotacao), cancellationToken: ct);
                        return "Inserted all documents";
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
                    var result = session.WithTransaction((s, ct) =>
                    {
                        anotacoesCollection.InsertOne(s, new BsonDocument("Nota", "Segunda nota"), cancellationToken: ct);
                        return "Inserted all documents";
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
                    var result = session.WithTransaction((s, ct) =>
                    {
                        anotacoesCollection.InsertOne(s, new BsonDocument("Nota", "Segunda nota"), cancellationToken: ct);
                        return "Inserted all documents";
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
                    var result = session.WithTransaction((s, ct) =>
                    {
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, segundaAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, terceiraAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, quartaAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, quintaAnotacao), cancellationToken: ct);
                        return "Inserted all documents";
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

        [Fact]
        public void TryInsertMany_WhenDatabaseExist_InsertAllDocuments()
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
                    var result = session.WithTransaction((s, ct) =>
                    {
                        var listaAnotacoes = new List<BsonDocument>();
                        listaAnotacoes.Add(new BsonDocument(campo, segundaAnotacao));
                        listaAnotacoes.Add(new BsonDocument(campo, terceiraAnotacao));
                        listaAnotacoes.Add(new BsonDocument(campo, quartaAnotacao));
                        listaAnotacoes.Add(new BsonDocument(campo, quintaAnotacao));

                        anotacoesCollection.InsertMany(listaAnotacoes);
                        return "Inserted all documents";
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

        [Fact]
        public void TryInsertOne5DocumentsAndCountDocumentsInsideTransation_WhenDatabaseExist_DontReturn5DocumentsButInsertDocuments()
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
            var countDocumentsInsideTransaction = 0L;

            // Act
            using (var session = ClientReplicaSet.StartSession())
            {
                try
                {
                    var result = session.WithTransaction((s, ct) =>
                    {
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, segundaAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, terceiraAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, quartaAnotacao), cancellationToken: ct);
                        anotacoesCollection.InsertOne(s, new BsonDocument(campo, quintaAnotacao), cancellationToken: ct);

                        countDocumentsInsideTransaction = anotacoesCollection.Find(new BsonDocument()).CountDocuments();

                        return "Inserted all documents";
                    }, null, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument());
            anotacoes.CountDocuments().Should().Be(5);

            countDocumentsInsideTransaction.Should().NotBe(5);

            exception.Should().BeNull();
        }
        #endregion

        #region Inserts using async methods
        [Fact]
        public async Task TryInsertOneAsync_WhenDatabaseExist_InsertDocumentsAsync()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            anotacoesCollection.InsertOne(new BsonDocument(campo, primeiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    
                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument(campo, segundaAnotacao));

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
            anotacoes.Count.Should().Be(2);

            anotacoesCollection.Find(new BsonDocument(campo, primeiraAnotacao)).CountDocuments().Should().Be(1);
            anotacoesCollection.Find(new BsonDocument(campo, segundaAnotacao)).CountDocuments().Should().Be(1);

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryInsertOneAsync_WhenDatabaseServerIsStandalone_ReturnException()
        {
            // Arrange
            var anotacoesCollection = DatabaseStandalone.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument("Nota", "Primeira nota"));
            Exception exception = null;

            // Act
            using (var session = await ClientStandalone.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument("Nota", "Segunda nota"));

                    await session.CommitTransactionAsync();
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
        public async Task TryInsertOneAsync_WhenCollectionDontExist_ReturnExceptionAsync()
        {
            // Arrange
            var collectionAnotacoes = "Anotacoes";
            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>(collectionAnotacoes).WithWriteConcern(WriteConcern.WMajority);
            DatabaseReplicasetDbTest.CreateCollection("OtherCollection"); // Force create database
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    DatabaseReplicasetDbTest.CreateCollection(session, collectionAnotacoes); // Try create collection inside transaction

                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument("Nota", "Segunda nota"));

                    await session.CommitTransactionAsync();

                }
                catch (Exception ex)
                {
                    exception = ex;
                    await session.AbortTransactionAsync();
                }
            }

            //Assert
            exception.Should().NotBeNull();            
            UtilsTest.CollectionExists(DatabaseReplicasetDbTest, "OtherCollection").Should().BeTrue();
            UtilsTest.CollectionExists(DatabaseReplicasetDbTest, "Anotacoes").Should().BeFalse();
        }

        [Fact]
        public async Task TryInsertOneAsync_WhenDatabaseDontExist_ReturnExceptionAsync()
        {
            // Arrange
            var collectionAnotacoes = "Anotacoes";
            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>(collectionAnotacoes).WithWriteConcern(WriteConcern.WMajority);            
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument("Nota", "Segunda nota"));

                    await session.CommitTransactionAsync();
                    
                }
                catch (Exception ex)
                {
                    exception = ex;
                    await session.AbortTransactionAsync();
                }
            }

            //Assert
            exception.Should().NotBeNull();
            UtilsTest.CollectionExists(DatabaseReplicasetDbTest, collectionAnotacoes).Should().BeFalse();
        }

        [Fact]
        public async Task TryInsertOneAsyncMultiplesDocuments_WhenDatabaseExist_InsertDocuments()
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
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, segundaAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, terceiraAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, quartaAnotacao));
                    await anotacoesCollection.InsertOneAsync(session, new BsonDocument(campo, quintaAnotacao));

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
            anotacoes.Count.Should().Be(5);

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryInsertManyAsync_WhenDatabaseExist_InsertAllDocuments()
        {
            // Arrange
            var campo = "Nota";
            var primeiraAnotacao = "Primeira nota";
            var segundaAnotacao = "Segunda nota";
            var terceiraAnotacao = "Terceira nota";
            var quartaAnotacao = "Quarta nota";
            var quintaAnotacao = "Quinta nota";

            var listaAnotacoes = new List<BsonDocument>();
            listaAnotacoes.Add(new BsonDocument(campo, segundaAnotacao));
            listaAnotacoes.Add(new BsonDocument(campo, terceiraAnotacao));
            listaAnotacoes.Add(new BsonDocument(campo, quartaAnotacao));
            listaAnotacoes.Add(new BsonDocument(campo, quintaAnotacao));

            var anotacoesCollection = DatabaseReplicasetDbTest.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            await anotacoesCollection.InsertOneAsync(new BsonDocument(campo, primeiraAnotacao));
            Exception exception = null;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();

                    await anotacoesCollection.InsertManyAsync(session, listaAnotacoes);

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
            anotacoes.Count.Should().Be(5);

            exception.Should().BeNull();
        }

        [Fact]
        public async Task TryInsertOneAsyncDocumentsAndCountDocumentsInsideTransation_WhenDatabaseExist_CountDocumentstAfterInsert()
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
            Exception exception = null;
            var countDocumentsInsideTransaction = 0L;

            // Act
            using (var session = await ClientReplicaSet.StartSessionAsync())
            {
                try
                {
                    session.StartTransaction();
                    
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, segundaAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, terceiraAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, quartaAnotacao));
                    await anotacoesCollection.InsertOneAsync(session,new BsonDocument(campo, quintaAnotacao));

                    countDocumentsInsideTransaction = anotacoesCollection.Find(session, new BsonDocument()).CountDocuments();

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
            }

            //Assert
            var anotacoes = anotacoesCollection.Find(new BsonDocument());
            anotacoes.CountDocuments().Should().Be(5);

            countDocumentsInsideTransaction.Should().Be(5);

            exception.Should().BeNull();
        }
        #endregion
    }
}
