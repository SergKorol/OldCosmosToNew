using Microsoft.Azure.Cosmos.Table;

namespace Legacy
{
    public class LegacyCosmosTable
    {
        private readonly CloudTableClient _client;

        public LegacyCosmosTable(string connectionString)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            _client = storageAccount.CreateCloudTableClient();
        }
        
        public async Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(string tableName) where TEntity : TableEntity, new()
        {
            var table = GetCloudTable(tableName);

            var tableQuery = new TableQuery<TEntity>();

            var results = new List<TEntity>();

            TableContinuationToken? token = null;

            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(tableQuery, token).ConfigureAwait(false);

                token = segment.ContinuationToken;

                results.AddRange(segment);
            } while (token != null);

            return results;
        }
        
        public async Task<IEnumerable<TEntity>> GetByPartitionKeyAsync<TEntity>(string tableName, string partitionKey,
            string[]? columns = null) where TEntity : TableEntity, new()
        {
            var table = GetCloudTable(tableName);
            TableQuery<TEntity> tableQuery;
            if (columns != null)
            {
                tableQuery = new TableQuery<TEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey))
                    .Select(columns);
            }
            else
            {
                tableQuery = new TableQuery<TEntity>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            }
            var results = new List<TEntity>();
            TableContinuationToken token = null;
            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(tableQuery, token).ConfigureAwait(false);
                token = segment.ContinuationToken;
                results.AddRange(segment);
            } while (token != null);
            return results;
        }

        public async Task<TEntity> GetAsync<TEntity>(string tableName, string partitionKey, string rowKey)
            where TEntity : TableEntity
        {
            var table = GetCloudTable(tableName);
            var get = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);
            var result = await table.ExecuteAsync(get).ConfigureAwait(false);

            return (TEntity)result.Result;
        }
        
        public async Task InsertOrMergeAsync(string tableName, TableEntity entity)
        {
            var table = GetCloudTable(tableName);
            var insert = TableOperation.InsertOrMerge(entity);

            await table.ExecuteAsync(insert).ConfigureAwait(false);
        }
        
        public async Task InsertAsync(string tableName, TableEntity entity)
        {
            var table = _client.GetTableReference(tableName);
            var insert = TableOperation.Insert(entity);
            await table.ExecuteAsync(insert).ConfigureAwait(false);
        }
        
        public async Task InsertOrReplaceAsync(string tableName, TableEntity entity)
        {
            var table = GetCloudTable(tableName);
            var insert = TableOperation.InsertOrReplace(entity);
            await table.ExecuteAsync(insert).ConfigureAwait(false);
        }
        
        public async Task<IList<TableResult>> DeleteByPartitionKeyBatchAsync<TEntity>(string tableName,
            string partitionKey, string[]? columns = null) where TEntity : TableEntity, new()
        {
            IList<TableResult> results = new List<TableResult>();
            IEnumerable<TEntity> entities =
                await GetByPartitionKeyAsync<TEntity>(tableName, partitionKey, columns)
                    .ConfigureAwait(false);
            if (entities.Any())
            {
                results = await DeleteBatchAsync(tableName, entities).ConfigureAwait(false);
            }
            return results;
        }
        
        public async Task DeleteEntityAsync(string tableName, TableEntity entity)
        {
            var table = GetCloudTable(tableName);
            entity.ETag ??= "*";
            var delete = TableOperation.Delete(entity);
            await table.ExecuteAsync(delete).ConfigureAwait(false);
        }
        
        public async Task ClearTableAsync(string tableName)
        {
            var entities = await GetAllAsync<TableEntity>(tableName).ConfigureAwait(false);
            if (entities.Any())
            {
                await DeleteBatchAsync(tableName, entities).ConfigureAwait(false);
            }
        }
        
        public async Task DeleteTableAsync(string tableName)
        {
            var table = GetCloudTable(tableName);
            await table.DeleteAsync().ConfigureAwait(false);
        }
        
        private CloudTable GetCloudTable(string tableName)
        {
            var table = _client.GetTableReference(tableName);
            return table;
        }
        
        private async Task<IList<TableResult>> DeleteBatchAsync<TEntity>(string tableName, IEnumerable<TEntity> entities)
            where TEntity : TableEntity, new()
        {
            var table = GetCloudTable(tableName);
            var batchOperation = new TableBatchOperation();
            foreach (var e in entities)
            {
                e.ETag = e.ETag ?? "*";
                batchOperation.Delete(e);
            }

            return await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
        }
        
    }
}