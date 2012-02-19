#region Copyright (c) Lokad 2010-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Web;

    using Lokad.Cloud.Storage.Instrumentation;
    using Lokad.Cloud.Storage.Instrumentation.Events;
    using Lokad.Cloud.Storage.Shared.Monads;
    using Lokad.Cloud.Storage.Tables;

    using Microsoft.WindowsAzure.StorageClient;

    /// <summary>
    /// Implementation based on the Table Storage of Windows Azure.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class TableStorageProvider : ITableStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        /// The continuation next partition key token.
        /// </summary>
        private const string ContinuationNextPartitionKeyToken = "x-ms-continuation-NextPartitionKey";

        /// <summary>
        /// The continuation next row key token.
        /// </summary>
        private const string ContinuationNextRowKeyToken = "x-ms-continuation-NextRowKey";

        /// <summary>
        /// The max entity transaction count.
        /// </summary>
        /// <remarks>
        /// HACK: those tokens will probably be provided as constants in the StorageClient library
        /// </remarks>
        private const int MaxEntityTransactionCount = 100;

        /// <summary>
        /// The max entity transaction payload.
        /// </summary>
        /// <remarks>
        /// HACK: Lowering the maximal payload, to avoid corner cases #117 (ContentLengthExceeded) [vermorel] 128kB is purely arbitrary, only taken as a reasonable safety margin 4 MB - 128kB
        /// </remarks>
        private const int MaxEntityTransactionPayload = 4 * 1024 * 1024 - 128 * 1024;

        /// <summary>
        /// The next partition key token.
        /// </summary>
        private const string NextPartitionKeyToken = "NextPartitionKey";

        /// <summary>
        /// The next row key token.
        /// </summary>
        private const string NextRowKeyToken = "NextRowKey";

        /// <summary>
        /// The observer.
        /// </summary>
        private readonly IStorageObserver observer;

        /// <summary>
        /// The policies.
        /// </summary>
        private readonly RetryPolicies policies;

        /// <summary>
        /// The serializer.
        /// </summary>
        private readonly IDataSerializer serializer;

        /// <summary>
        /// The table storage.
        /// </summary>
        private readonly CloudTableClient tableStorage;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorageProvider"/> class. 
        /// IoC constructor.
        /// </summary>
        /// <param name="tableStorage">
        /// The table storage. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="observer">
        /// The observer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public TableStorageProvider(
            CloudTableClient tableStorage, IDataSerializer serializer, IStorageObserver observer = null)
        {
            this.policies = new RetryPolicies(observer);
            this.tableStorage = tableStorage;
            this.serializer = serializer;
            this.observer = observer;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Performs lazy splitting of the provided collection into collections of <paramref name="sliceLength"/>
        /// </summary>
        /// <typeparam name="TItem">
        /// The type of the item. 
        /// </typeparam>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <param name="sliceLength">
        /// Maximum length of the slice. 
        /// </param>
        /// <returns>
        /// lazy enumerator of the collection of arrays 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<TItem[]> Slice<TItem>(IEnumerable<TItem> source, int sliceLength)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            if (sliceLength <= 0)
            {
                throw new ArgumentOutOfRangeException("sliceLength", "value must be greater than 0");
            }

            var list = new List<TItem>(sliceLength);
            foreach (var item in source)
            {
                list.Add(item);
                if (sliceLength == list.Count)
                {
                    yield return list.ToArray();
                    list.Clear();
                }
            }

            if (list.Count > 0)
            {
                yield return list.ToArray();
            }
        }

        /// <summary>
        /// Creates a new table if it does not exist already.
        /// </summary>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <returns>
        /// <c>true</c> if a new table has been created. <c>false</c> if the table already exists. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool CreateTable(string tableName)
        {
            var flag = false;
            Retry.Do(
                this.policies.SlowInstantiation, 
                CancellationToken.None, 
                () => flag = this.tableStorage.CreateTableIfNotExist(tableName));

            return flag;
        }

        /// <summary>
        /// Deletes all specified entities.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="partitionKey">
        /// The partition key (assumed to be non null). 
        /// </param>
        /// <param name="rowKeys">
        /// Lazy enumeration of non null string representing the row keys. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Delete<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            this.DeleteInternal<T>(tableName, partitionKey, rowKeys.Select(k => Tuple.Create(k, "*")), true);
        }

        /// <summary>
        /// Deletes a collection of entities.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <param name="force">
        /// if set to <c>true</c> [force]. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Delete<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                this.DeleteInternal<T>(
                    tableName, g.Key, g.Select(e => Tuple.Create(e.RowKey, MapETag(e.ETag, force))), force);
            }
        }

        /// <summary>
        /// Deletes a table if it exists.
        /// </summary>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the table has been deleted. <c>false</c> if the table does not exist. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool DeleteTable(string tableName)
        {
            var flag = false;
            Retry.Do(
                this.policies.SlowInstantiation, 
                CancellationToken.None, 
                () => flag = this.tableStorage.DeleteTableIfExist(tableName));

            return flag;
        }

        /// <summary>
        /// Iterates through all entities of a given table.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <returns>
        /// The enumerable of cloud entities of T. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName)
        {
            if (null == tableName)
            {
                throw new ArgumentNullException("tableName");
            }

            var context = this.tableStorage.GetDataServiceContext();
            return this.GetInternal<T>(context, tableName, Maybe<string>.Empty);
        }

        /// <summary>
        /// Iterates through all entities of a given table and partition.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <returns>
        /// The enumerable of cloud entities of T. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey)
        {
            if (null == tableName)
            {
                throw new ArgumentNullException("tableName");
            }

            if (null == partitionKey)
            {
                throw new ArgumentNullException("partitionKey");
            }

            if (partitionKey.Contains("'"))
            {
                throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char in partitionKey.");
            }

            var filter = string.Format("(PartitionKey eq '{0}')", HttpUtility.UrlEncode(partitionKey));

            var context = this.tableStorage.GetDataServiceContext();
            return this.GetInternal<T>(context, tableName, filter);
        }

        /// <summary>
        /// Iterates through a range of entities of a given table and partition.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the Table. 
        /// </param>
        /// <param name="partitionKey">
        /// Name of the partition which can not be null. 
        /// </param>
        /// <param name="startRowKey">
        /// Inclusive start row key. If <c>null</c> , no start range constraint is enforced. 
        /// </param>
        /// <param name="endRowKey">
        /// Exclusive end row key. If <c>null</c> , no ending range constraint is enforced. 
        /// </param>
        /// <returns>
        /// The enumerable of cloud entities of T. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(
            string tableName, string partitionKey, string startRowKey, string endRowKey)
        {
            if (null == tableName)
            {
                throw new ArgumentNullException("tableName");
            }

            if (null == partitionKey)
            {
                throw new ArgumentNullException("partitionKey");
            }

            if (partitionKey.Contains("'"))
            {
                throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char.");
            }

            if (startRowKey != null && startRowKey.Contains("'"))
            {
                throw new ArgumentOutOfRangeException("startRowKey", "Incorrect char.");
            }

            if (endRowKey != null && endRowKey.Contains("'"))
            {
                throw new ArgumentOutOfRangeException("endRowKey", "Incorrect char.");
            }

            var filter = string.Format("(PartitionKey eq '{0}')", HttpUtility.UrlEncode(partitionKey));

            // optional starting range constraint
            if (!string.IsNullOrEmpty(startRowKey))
            {
                // ge = GreaterThanOrEqual (inclusive)
                filter += string.Format(" and (RowKey ge '{0}')", HttpUtility.UrlEncode(startRowKey));
            }

            if (!string.IsNullOrEmpty(endRowKey))
            {
                // lt = LessThan (exclusive)
                filter += string.Format(" and (RowKey lt '{0}')", HttpUtility.UrlEncode(endRowKey));
            }

            var context = this.tableStorage.GetDataServiceContext();
            return this.GetInternal<T>(context, tableName, filter);
        }

        /// <summary>
        /// Iterates through all entities specified by their row keys.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// The name of the table. This table should exists otherwise the method will fail. 
        /// </param>
        /// <param name="partitionKey">
        /// Partition key (can not be null). 
        /// </param>
        /// <param name="rowKeys">
        /// lazy enumeration of non null string representing rowKeys. 
        /// </param>
        /// <returns>
        /// The enumerable of cloud entities of T. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            if (null == tableName)
            {
                throw new ArgumentNullException("tableName");
            }

            if (null == partitionKey)
            {
                throw new ArgumentNullException("partitionKey");
            }

            if (partitionKey.Contains("'"))
            {
                throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char.");
            }

            var context = this.tableStorage.GetDataServiceContext();

            foreach (var slice in Slice(rowKeys, MaxEntityTransactionCount))
            {
                // work-around the limitation of ADO.NET that does not provide a native way
                // of query a set of specified entities directly.
                var builder = new StringBuilder();
                builder.Append(string.Format("(PartitionKey eq '{0}') and (", HttpUtility.UrlEncode(partitionKey)));
                for (var i = 0; i < slice.Length; i++)
                {
                    // in order to avoid SQL-injection-like problems 
                    if (slice[i].Contains("'"))
                    {
                        throw new ArgumentOutOfRangeException("rowKeys", "Incorrect char.");
                    }

                    builder.Append(string.Format("(RowKey eq '{0}')", HttpUtility.UrlEncode(slice[i])));
                    if (i < slice.Length - 1)
                    {
                        builder.Append(" or ");
                    }
                }

                builder.Append(")");

                foreach (var entity in this.GetInternal<T>(context, tableName, builder.ToString()))
                {
                    yield return entity;
                }
            }
        }

        /// <summary>
        /// Returns the list of all the tables that exist in the storage.
        /// </summary>
        /// <returns>
        /// The names of the tables. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> GetTables()
        {
            return this.tableStorage.ListTables();
        }

        /// <summary>
        /// Inserts a collection of new entities into the table storage.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// if an already existing entity has been encountered.
        /// </exception>
        /// <remarks>
        /// </remarks>
        public void Insert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                this.InsertInternal(tableName, g);
            }
        }

        /// <summary>
        /// Updates a collection of existing entities into the table storage.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <param name="force">
        /// if set to <c>true</c> [force]. 
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// thrown if the table does not exist
        ///   or an non-existing entity has been encountered.
        /// </exception>
        /// <remarks>
        /// </remarks>
        public void Update<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                this.UpdateInternal(tableName, g, force);
            }
        }

        /// <summary>
        /// Updates or insert a collection of existing entities into the table storage.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the entities. 
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Upsert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                this.UpsertInternal(tableName, g);
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Maps the E tag.
        /// </summary>
        /// <param name="etag">
        /// The etag. 
        /// </param>
        /// <param name="force">
        /// if set to <c>true</c> [force]. 
        /// </param>
        /// <returns>
        /// The map e tag.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static string MapETag(string etag, bool force)
        {
            return force || string.IsNullOrEmpty(etag) ? "*" : etag;
        }

        /// <summary>
        /// Reads the E tags and detach.
        /// </summary>
        /// <param name="context">
        /// The context. 
        /// </param>
        /// <param name="write">
        /// The write. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private static void ReadETagsAndDetach(DataServiceContext context, Action<object, string> write)
        {
            foreach (var entity in context.Entities)
            {
                write(entity.Entity, entity.ETag);
                context.Detach(entity.Entity);
            }
        }

        /// <summary>
        /// Resolves the type of the fat entity.
        /// </summary>
        /// <param name="name">
        /// The name. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static Type ResolveFatEntityType(string name)
        {
            return typeof(FatEntity);
        }

        /// <summary>
        /// Slice entities according the payload limitation of the transaction group, plus the maximal number of entities to be embedded into a single transaction.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <param name="getPayload">
        /// The get payload. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static IEnumerable<T[]> SliceEntities<T>(IEnumerable<T> entities, Func<T, int> getPayload)
        {
            var accumulator = new List<T>(100);
            var payload = 0;
            foreach (var entity in entities)
            {
                var entityPayLoad = getPayload(entity);

                if (accumulator.Count >= MaxEntityTransactionCount
                    || payload + entityPayLoad >= MaxEntityTransactionPayload)
                {
                    yield return accumulator.ToArray();
                    accumulator.Clear();
                    payload = 0;
                }

                accumulator.Add(entity);
                payload += entityPayLoad;
            }

            if (accumulator.Count > 0)
            {
                yield return accumulator.ToArray();
            }
        }

        /// <summary>
        /// Deletes the internal.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <param name="rowKeysAndETags">
        /// The row keys and E tags. 
        /// </param>
        /// <param name="force">
        /// if set to <c>true</c> [force]. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void DeleteInternal<T>(
            string tableName, string partitionKey, IEnumerable<Tuple<string, string>> rowKeysAndETags, bool force)
        {
            var context = this.tableStorage.GetDataServiceContext();

            var stopwatch = new Stopwatch();

            // CAUTION: make sure to get rid of potential duplicate in rowkeys.
            // (otherwise insertion in 'context' is likely to fail)
            foreach (var s in Slice(
                rowKeysAndETags
                    
                    // Similar effect than 'Distinct' based on 'RowKey'
                    .ToLookup(p => p.Item1, p => p).Select(g => g.First()), 
                MaxEntityTransactionCount))
            {
                stopwatch.Restart();

                var slice = s;

                DeletionStart: // 'slice' might have been refreshed if some entities were already deleted

                foreach (var rowKeyAndETag in slice)
                {
                    // Deleting entities in 1 roundtrip
                    // http://blog.smarx.com/posts/deleting-entities-from-windows-azure-without-querying-first
                    var mock = new FatEntity { PartitionKey = partitionKey, RowKey = rowKeyAndETag.Item1 };

                    context.AttachTo(tableName, mock, rowKeyAndETag.Item2);
                    context.DeleteObject(mock);
                }

                try
                {
                    // HACK: [vermorel] if a single entity is missing, then the whole batch operation is aborded
                    try
                    {
                        // HACK: nested try/catch to handle the special case where the table is missing
                        Retry.Do(
                            this.policies.TransientTableErrorBackOff, 
                            CancellationToken.None, 
                            () => context.SaveChanges(SaveChangesOptions.Batch));
                    }
                    catch (DataServiceRequestException ex)
                    {
                        // if the table is missing, no need to go on with the deletion
                        var errorCode = RetryPolicies.GetErrorCode(ex);
                        if (TableErrorCodeStrings.TableNotFound == errorCode)
                        {
                            this.NotifySucceeded(StorageOperationType.TableDelete, stopwatch);
                            return;
                        }

                        throw;
                    }
                }
                    
                    // if some entities exist
                catch (DataServiceRequestException ex)
                {
                    var errorCode = RetryPolicies.GetErrorCode(ex);

                    // HACK: Table Storage both implement a bizarre non-idempotent semantic
                    // but in addition, it throws a non-documented exception as well. 
                    if (errorCode != "ResourceNotFound")
                    {
                        throw;
                    }

                    slice =
                        this.Get<T>(tableName, partitionKey, slice.Select(p => p.Item1)).Select(
                            e => Tuple.Create(e.RowKey, MapETag(e.ETag, force))).ToArray();

                    // entities with same name will be added again
                    context = this.tableStorage.GetDataServiceContext();

                    // HACK: [vermorel] yes, gotos are horrid, but other solutions are worst here.
                    goto DeletionStart;
                }

                this.NotifySucceeded(StorageOperationType.TableDelete, stopwatch);
            }
        }

        /// <summary>
        /// Gets the internal.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="context">
        /// The context. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="filter">
        /// The filter. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private IEnumerable<CloudEntity<T>> GetInternal<T>(
            TableServiceContext context, string tableName, Maybe<string> filter)
        {
            string continuationRowKey = null;
            string continuationPartitionKey = null;

            var stopwatch = Stopwatch.StartNew();

            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            do
            {
                var query = context.CreateQuery<FatEntity>(tableName);

                if (filter.HasValue)
                {
                    query = query.AddQueryOption("$filter", filter.Value);
                }

                if (null != continuationRowKey)
                {
                    query =
                        query.AddQueryOption(NextRowKeyToken, continuationRowKey).AddQueryOption(
                            NextPartitionKeyToken, continuationPartitionKey);
                }

                QueryOperationResponse response = null;
                FatEntity[] fatEntities = null;

                Retry.Do(
                    this.policies.TransientTableErrorBackOff, 
                    CancellationToken.None, 
                    () =>
                        {
                            try
                            {
                                response = query.Execute() as QueryOperationResponse;
                                fatEntities = ((IEnumerable<FatEntity>)response).ToArray();
                            }
                            catch (DataServiceQueryException ex)
                            {
                                // if the table does not exist, there is nothing to return
                                var errorCode = RetryPolicies.GetErrorCode(ex);
                                if (TableErrorCodeStrings.TableNotFound == errorCode
                                    || StorageErrorCodeStrings.ResourceNotFound == errorCode)
                                {
                                    fatEntities = new FatEntity[0];
                                    return;
                                }

                                throw;
                            }
                        });

                this.NotifySucceeded(StorageOperationType.TableQuery, stopwatch);

                foreach (var fatEntity in fatEntities)
                {
                    var etag = context.Entities.First(e => e.Entity == fatEntity).ETag;
                    context.Detach(fatEntity);
                    yield return FatEntity.Convert<T>(fatEntity, this.serializer, etag);
                }

                Debug.Assert(context.Entities.Count == 0);

                if (null != response && response.Headers.ContainsKey(ContinuationNextRowKeyToken))
                {
                    continuationRowKey = response.Headers[ContinuationNextRowKeyToken];
                    continuationPartitionKey = response.Headers[ContinuationNextPartitionKeyToken];

                    stopwatch.Restart();
                }
                else
                {
                    continuationRowKey = null;
                    continuationPartitionKey = null;
                }
            }
            while (null != continuationRowKey);
        }

        /// <summary>
        /// Inserts the internal.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void InsertInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            var context = this.tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(FatEntity.Convert(e, this.serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    context.AddObject(tableName, fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(
                    this.policies.TransientTableErrorBackOff, 
                    CancellationToken.None, 
                    () =>
                        {
                            try
                            {
                                // HACK: nested try/catch
                                try
                                {
                                    context.SaveChanges(
                                        noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                }
                                    
                                    // special casing the need for table instantiation
                                catch (DataServiceRequestException ex)
                                {
                                    var errorCode = RetryPolicies.GetErrorCode(ex);
                                    if (errorCode == TableErrorCodeStrings.TableNotFound
                                        || errorCode == StorageErrorCodeStrings.ResourceNotFound)
                                    {
                                        Retry.Do(
                                            this.policies.SlowInstantiation, 
                                            CancellationToken.None, 
                                            () =>
                                                {
                                                    try
                                                    {
                                                        this.tableStorage.CreateTableIfNotExist(tableName);
                                                    }
                                                        
                                                        // HACK: incorrect behavior of the StorageClient (2010-09)
                                                        // Fails to behave properly in multi-threaded situations
                                                    catch (StorageClientException cex)
                                                    {
                                                        if (cex.ExtendedErrorInformation == null
                                                            ||
                                                            cex.ExtendedErrorInformation.ErrorCode
                                                            != TableErrorCodeStrings.TableAlreadyExists)
                                                        {
                                                            throw;
                                                        }
                                                    }

                                                    context.SaveChanges(
                                                        noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                                    ReadETagsAndDetach(
                                                        context, 
                                                        (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                                });
                                    }
                                    else
                                    {
                                        throw;
                                    }
                                }
                            }
                            catch (DataServiceRequestException ex)
                            {
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges();
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                    
                                    // HACK: undocumented code returned by the Table Storage
                                else if (errorCode == "ContentLengthExceeded")
                                {
                                    context.SaveChanges();
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                            catch (DataServiceQueryException ex)
                            {
                                // HACK: code duplicated
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges();
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        });

                this.NotifySucceeded(StorageOperationType.TableInsert, stopwatch);
            }
        }

        /// <summary>
        /// Notifies the succeeded.
        /// </summary>
        /// <param name="operationType">
        /// Type of the operation. 
        /// </param>
        /// <param name="stopwatch">
        /// The stopwatch. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void NotifySucceeded(StorageOperationType operationType, Stopwatch stopwatch)
        {
            if (this.observer != null)
            {
                this.observer.Notify(new StorageOperationSucceededEvent(operationType, stopwatch.Elapsed));
            }
        }

        /// <summary>
        /// Updates the internal.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <param name="force">
        /// if set to <c>true</c> [force]. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void UpdateInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            var context = this.tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(FatEntity.Convert(e, this.serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    // entities should be updated in a single round-trip
                    context.AttachTo(tableName, fatEntity.Item1, MapETag(fatEntity.Item2.ETag, force));
                    context.UpdateObject(fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(
                    this.policies.TransientTableErrorBackOff, 
                    CancellationToken.None, 
                    () =>
                        {
                            try
                            {
                                context.SaveChanges(noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                ReadETagsAndDetach(
                                    context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            }
                            catch (DataServiceRequestException ex)
                            {
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges();
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else if (errorCode == TableErrorCodeStrings.TableNotFound)
                                {
                                    Retry.Do(
                                        this.policies.SlowInstantiation, 
                                        CancellationToken.None, 
                                        () =>
                                            {
                                                try
                                                {
                                                    this.tableStorage.CreateTableIfNotExist(tableName);
                                                }
                                                    
                                                    // HACK: incorrect behavior of the StorageClient (2010-09)
                                                    // Fails to behave properly in multi-threaded situations
                                                catch (StorageClientException cex)
                                                {
                                                    if (cex.ExtendedErrorInformation.ErrorCode
                                                        != TableErrorCodeStrings.TableAlreadyExists)
                                                    {
                                                        throw;
                                                    }
                                                }

                                                context.SaveChanges(
                                                    noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                                ReadETagsAndDetach(
                                                    context, 
                                                    (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                            });
                                }
                                else if (errorCode == StorageErrorCodeStrings.ResourceNotFound)
                                {
                                    throw new InvalidOperationException(
                                        "Cannot call update on a resource that does not exist", ex);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                            catch (DataServiceQueryException ex)
                            {
                                // HACK: code duplicated
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges();
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        });

                this.NotifySucceeded(StorageOperationType.TableUpdate, stopwatch);
            }
        }

        /// <summary>
        /// Upserts the internal.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <remarks>
        /// Upsert is making several storage calls to emulate the missing semantic from the Table Storage.
        /// </remarks>
        private void UpsertInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            if (this.tableStorage.BaseUri.Host == "127.0.0.1")
            {
                // HACK: Dev Storage of v1.6 tools does NOT support Upsert yet -> emulate

                // checking for entities that already exist
                var partitionKey = entities.First().PartitionKey;
                var existingKeys =
                    new HashSet<string>(
                        this.Get<T>(tableName, partitionKey, entities.Select(e => e.RowKey)).Select(e => e.RowKey));

                // inserting or updating depending on the presence of the keys
                this.Insert(tableName, entities.Where(e => !existingKeys.Contains(e.RowKey)));
                this.Update(tableName, entities.Where(e => existingKeys.Contains(e.RowKey)), true);

                return;
            }

            var context = this.tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(FatEntity.Convert(e, this.serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    // entities should be updated in a single round-trip
                    context.AttachTo(tableName, fatEntity.Item1);
                    context.UpdateObject(fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(
                    this.policies.TransientTableErrorBackOff, 
                    CancellationToken.None, 
                    () =>
                        {
                            try
                            {
                                context.SaveChanges(
                                    noBatchMode
                                        ? SaveChangesOptions.ReplaceOnUpdate
                                        : SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch);
                                ReadETagsAndDetach(
                                    context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            }
                            catch (DataServiceRequestException ex)
                            {
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges(SaveChangesOptions.ReplaceOnUpdate);
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else if (errorCode == TableErrorCodeStrings.TableNotFound)
                                {
                                    Retry.Do(
                                        this.policies.SlowInstantiation, 
                                        CancellationToken.None, 
                                        () =>
                                            {
                                                try
                                                {
                                                    this.tableStorage.CreateTableIfNotExist(tableName);
                                                }
                                                    
                                                    // HACK: incorrect behavior of the StorageClient (2010-09)
                                                    // Fails to behave properly in multi-threaded situations
                                                catch (StorageClientException cex)
                                                {
                                                    if (cex.ExtendedErrorInformation.ErrorCode
                                                        != TableErrorCodeStrings.TableAlreadyExists)
                                                    {
                                                        throw;
                                                    }
                                                }

                                                context.SaveChanges(
                                                    noBatchMode
                                                        ? SaveChangesOptions.ReplaceOnUpdate
                                                        : SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch);
                                                ReadETagsAndDetach(
                                                    context, 
                                                    (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                            });
                                }
                                else if (errorCode == StorageErrorCodeStrings.ResourceNotFound)
                                {
                                    throw new InvalidOperationException(
                                        "Cannot call update on a resource that does not exist", ex);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                            catch (DataServiceQueryException ex)
                            {
                                // HACK: code duplicated
                                var errorCode = RetryPolicies.GetErrorCode(ex);

                                if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                                {
                                    // if batch does not work, then split into elementary requests
                                    // PERF: it would be better to split the request in two and retry
                                    context.SaveChanges(SaveChangesOptions.ReplaceOnUpdate);
                                    ReadETagsAndDetach(
                                        context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    noBatchMode = true;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        });

                this.NotifySucceeded(StorageOperationType.TableUpsert, stopwatch);
            }
        }

        #endregion
    }
}