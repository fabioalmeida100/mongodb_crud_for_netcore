using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDbCRUD.Models
{
    public class NotasImportantes
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string Nota { get; set; }        
    }
}
