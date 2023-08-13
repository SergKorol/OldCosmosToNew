using Azure;
using Azure.Data.Tables;

namespace Modern;

public class ModernCosmosTable
{
    private readonly TableServiceClient _client;

    public ModernCosmosTable(string connectionString)
    {
        _client = new TableServiceClient(connectionString);
    }
    
    public async Task<TableClient> CreateTableIfToExistsAsync(string tableName)
    {
        var table = _client.GetTableClient(tableName);
        await table.CreateIfNotExistsAsync().ConfigureAwait(false);
        return table;
    }
    
    public async Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(string tableName)
        where TEntity : class, ITableEntity, new()
    {
        TableClient tableClient = GetTableClient(tableName);

        var result = new List<TEntity>();

        await foreach (var entity in tableClient.QueryAsync<TEntity>())
        {
            result.Add(entity);
        }
        return result;
    }
    
    public async Task<IEnumerable<TEntity>> GetByPartitionKeyAsync<TEntity>(string tableName, string partitionKey,
        string[]? columns = null) where TEntity : class, ITableEntity, new()
    {
        TableClient tableClient = GetTableClient(tableName);

        var results = new List<TEntity>();
        if (columns != null)
        {
            await foreach (var entity in tableClient.QueryAsync<TEntity>(filter => filter.PartitionKey == partitionKey,
                               select: columns))
            {
                results.Add(entity);
            }
        }
        else
        {
            await foreach (var entity in tableClient.QueryAsync<TEntity>(filter => filter.PartitionKey == partitionKey))
            {
                results.Add(entity);
            }
        }
        return results;
    }
    
    public async Task<TEntity> GetAsync<TEntity>(string tableName, string partitionKey, string rowKey)
        where TEntity : class, ITableEntity
    {
        TableClient tableClient = GetTableClient(tableName);
        var result = await tableClient.GetEntityAsync<TEntity>(partitionKey, rowKey).ConfigureAwait(false);
        return result;
    }
    
    public async Task InsertOrMergeAsync(string tableName, ITableEntity entity)
    {
        TableClient tableClient = GetTableClient(tableName);
        await tableClient.UpsertEntityAsync(entity);
    }
    
    public async Task InsertAsync(string tableName, ITableEntity entity)
    {
        TableClient tableClient = GetTableClient(tableName);
        await tableClient.UpdateEntityAsync(entity, entity.ETag);
    }
    
    public async Task InsertOrReplaceAsync(string tableName, ITableEntity entity)
    {
        TableClient tableClient = GetTableClient(tableName);
        await tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace);
    }
    
    public async Task<Response<IReadOnlyList<Response>>?> DeleteByPartitionKeyBatchAsync<TEntity>(string tableName,
        string partitionKey, string[]? columns = null) where TEntity : class, ITableEntity, new()
    {
        Response<IReadOnlyList<Response>>? results;
        IEnumerable<TEntity> entities = await GetByPartitionKeyAsync<TEntity>(tableName, partitionKey, columns)
            .ConfigureAwait(false);
        if (!entities.Any()) return null;
        results = await DeleteBatchAsync(tableName, entities).ConfigureAwait(false);
        return results;
    }
    
    public async Task DeleteEntityAsync(string tableName, ITableEntity entity)
    {
        TableClient tableClient = GetTableClient(tableName);
        await tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey, entity.ETag).ConfigureAwait(false);
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
        var table = GetTableClient(tableName);
        await table.DeleteAsync().ConfigureAwait(false);
    }
    
    private TableClient GetTableClient(string tableName)
    {
        var tableClient = _client.GetTableClient(tableName);
        return tableClient;
    }
    
    private async Task<Response<IReadOnlyList<Response>>?> DeleteBatchAsync<TEntity>(string tableName,
        IEnumerable<TEntity> entities) where TEntity : ITableEntity, new()
    {
        TableClient tableClient = GetTableClient(tableName);
        var batch = new List<TableTransactionAction>();

        foreach (var entity in entities)
        {
            batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity));
        }

        return await tableClient.SubmitTransactionAsync(batch).ConfigureAwait(false);
    }
}