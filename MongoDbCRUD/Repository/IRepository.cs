using MongoDB.Driver;
using MongoDbCRUD.Models;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MongoDbCRUD.Repository
{
    public interface IRepository<T> where T : BaseEntity
    {
        void Create(T entity);
        Task<IList<T>> GetAll();
        Task<T> GetById(string id);
        void UpdateById(T entity);
        void DeleteById(string id);
    }
}
