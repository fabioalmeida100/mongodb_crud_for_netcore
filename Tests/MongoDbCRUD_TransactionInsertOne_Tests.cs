using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Tests.TestUtils;
using Xunit;
using FluentAssertions;

namespace Tests.TryInsertOne
{
    public class MongoDbCRUD_TransactionInsertOne_Tests: TestFixture
    {
        [Fact]
        public void TryInsertOne_When_DatabaseDontExist()
        {
            // Arrange
            var anotacoesCollection = Database.GetCollection<BsonDocument>("Anotacoes").WithWriteConcern(WriteConcern.WMajority);
            var exception = new Exception();

            // Act
            using (var session = Client.StartSession())
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
    }
}
