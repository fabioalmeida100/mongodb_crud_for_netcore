using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDbCRUD.Models
{
    public abstract class BaseEntity
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }        
    }
}
