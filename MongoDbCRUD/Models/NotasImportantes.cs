using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDbCRUD.Models
{
    [BsonCollection("NotasImportantes")]
    public class NotasImportantes: BaseEntity
    {        
        public string Nota { get; set; }        
    }
}
