using DDS.Domain.Core.Abstractions.Model.Entities;
using DDS.Domain.Core.Abstractions.Repositories;
using Flunt.Notifications;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DDS.Infra.Data.Mongo.Abstractions.Repositories
{
    public abstract class MongoRepository<TEntity> : Notifiable, IRepository<TEntity>
        where TEntity : Entity
    {
        private protected readonly IMongoCollection<TEntity> _mongoCollection;
        private protected readonly IClientSessionHandle _session;
        private protected readonly IMongoClient mongoClient;

        protected MongoRepository(IMongoDatabase mongoDatabase, IClientSessionHandle session)
        {
            mongoClient = mongoDatabase.Client;
            _session = session;

            List<string> collectionNames = mongoDatabase.ListCollectionNames().ToList();

            if (!collectionNames.Any(d => d == $"{typeof(TEntity).Name}"))
                mongoDatabase.CreateCollection(typeof(TEntity).Name);

            this._mongoCollection = mongoDatabase.GetCollection<TEntity>(typeof(TEntity).Name);
        }

        public async Task Adicionar(TEntity entity)
        {
            await _mongoCollection.InsertOneAsync(_session, entity);
        }

        public async Task Atualizar(TEntity entity)
        {
            var filter = Builders<TEntity>.Filter.Eq(doc => doc.Id, entity.Id);
            await _mongoCollection.FindOneAndReplaceAsync(_session, filter, entity);
        }

        public IQueryable<TEntity> AsQueryable()
        {
            return _mongoCollection.AsQueryable();
        }

        public bool ConsultarSeExiste(Guid id)
            => ConsultarPorId(id).Result != null;

        public async Task<TEntity> ConsultarPorId(Guid id)
        {
            return (await _mongoCollection.FindAsync(_session, entity => entity.Id == id)).FirstOrDefault();
        }

        public async Task Excluir(Guid id)
        {
            var entity = await ConsultarPorId(id);

            if (entity == null)
                return;

            var filter = Builders<TEntity>.Filter.Eq(doc => doc.Id, id);
            await _mongoCollection.FindOneAndDeleteAsync(_session, filter);
        }

    }
}
