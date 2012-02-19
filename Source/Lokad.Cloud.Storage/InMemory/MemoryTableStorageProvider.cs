#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Globalization;
    using System.Linq;

    using Lokad.Cloud.Storage.Azure;
    using Lokad.Cloud.Storage.Tables;

    /// <summary>
    /// Mock in-memory TableStorage Provider.
    /// </summary>
    /// <remarks>
    /// All the methods of <see cref="MemoryTableStorageProvider"/> are thread-safe.
    /// </remarks>
    public class MemoryTableStorageProvider : ITableStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        ///   naive global lock to make methods thread-safe.
        /// </summary>
        private readonly object syncRoot;

        /// <summary>
        ///   In memory table storage : entries per table (designed for simplicity instead of performance)
        /// </summary>
        private readonly Dictionary<string, List<MockTableEntry>> tables;

        /// <summary>
        /// The next e tag.
        /// </summary>
        private int nextETag;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryTableStorageProvider"/> class. 
        ///   Constructor for <see cref="MemoryTableStorageProvider"/> .
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryTableStorageProvider()
        {
            this.tables = new Dictionary<string, List<MockTableEntry>>();
            this.syncRoot = new object();
            this.DataSerializer = new CloudFormatter();
        }

        #endregion

        #region Properties

        /// <summary>
        ///   Gets or sets formatter as requiered to handle FatEntities.
        /// </summary>
        /// <value> The data serializer. </value>
        /// <remarks>
        /// </remarks>
        internal IDataSerializer DataSerializer { get; set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Creates a new table if it does not exist already.
        /// </summary>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <returns>
        /// <c>true</c> if a new table has been created. <c>false</c> if the table already exists. 
        /// </returns>
        /// <see cref="ITableStorageProvider.CreateTable"/>
        /// <remarks>
        /// </remarks>
        public bool CreateTable(string tableName)
        {
            lock (this.syncRoot)
            {
                if (this.tables.ContainsKey(tableName))
                {
                    // If the table already exists: return false.
                    return false;
                }

                // create table return true.
                this.tables.Add(tableName, new List<MockTableEntry>());
                return true;
            }
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
        /// <see cref="ITableStorageProvider.Delete{T}(string,string,IEnumerable{string})"/>
        /// <remarks>
        /// </remarks>
        public void Delete<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            lock (this.syncRoot)
            {
                List<MockTableEntry> entries;
                if (!this.tables.TryGetValue(tableName, out entries))
                {
                    return;
                }

                var keys = new HashSet<string>(rowKeys);
                entries.RemoveAll(entry => entry.PartitionKey == partitionKey && keys.Contains(entry.RowKey));
            }
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
            lock (this.syncRoot)
            {
                List<MockTableEntry> entries;
                if (!this.tables.TryGetValue(tableName, out entries))
                {
                    return;
                }

                var entityList = entities.ToList();

                // verify valid data BEFORE deleting them
                if (
                    entityList.Join(entries, ToId, ToId, (u, v) => force || u.ETag == null || u.ETag == v.ETag).Any(c => !c))
                {
                    throw new DataServiceRequestException("DELETE: etag conflict.");
                }

                // ok, we can delete safely now
                entries.RemoveAll(
                    entry =>
                    entityList.Any(entity => entity.PartitionKey == entry.PartitionKey && entity.RowKey == entry.RowKey));
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
        /// <see cref="ITableStorageProvider.DeleteTable"/>
        /// <remarks>
        /// </remarks>
        public bool DeleteTable(string tableName)
        {
            lock (this.syncRoot)
            {
                if (this.tables.ContainsKey(tableName))
                {
                    // If the table exists remove it.
                    this.tables.Remove(tableName);
                    return true;
                }

                // Can not remove an unexisting table.
                return false;
            }
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
        /// <see cref="ITableStorageProvider.Get{T}(string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName)
        {
            return this.GetInternal<T>(tableName, entry => true);
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
        /// <see cref="ITableStorageProvider.Get{T}(string,string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey)
        {
            return this.GetInternal<T>(tableName, entry => entry.PartitionKey == partitionKey);
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
        /// <see cref="ITableStorageProvider.Get{T}(string,string,string,string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(
            string tableName, string partitionKey, string startRowKey, string endRowKey)
        {
            var isInRange = string.IsNullOrEmpty(endRowKey)
                                ? (Func<string, bool>)(rowKey => string.CompareOrdinal(startRowKey, rowKey) <= 0)
                                : (rowKey =>
                                   string.CompareOrdinal(startRowKey, rowKey) <= 0 && string.CompareOrdinal(rowKey, endRowKey) < 0);

            return
                this.GetInternal<T>(tableName, entry => entry.PartitionKey == partitionKey && isInRange(entry.RowKey)).OrderBy(entity => entity.RowKey);
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
        /// <see cref="ITableStorageProvider.Get{T}(string,string,System.Collections.Generic.IEnumerable{string})"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            var keys = new HashSet<string>(rowKeys);
            return this.GetInternal<T>(
                tableName, entry => entry.PartitionKey == partitionKey && keys.Contains(entry.RowKey));
        }

        /// <summary>
        /// Returns the list of all the tables that exist in the storage.
        /// </summary>
        /// <returns>
        /// The names of the tables. 
        /// </returns>
        /// <see cref="ITableStorageProvider.GetTables"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> GetTables()
        {
            lock (this.syncRoot)
            {
                return this.tables.Keys;
            }
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
        /// <see cref="ITableStorageProvider.Insert{T}"/>
        /// <remarks>
        /// </remarks>
        public void Insert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            lock (this.syncRoot)
            {
                List<MockTableEntry> entries;
                if (!this.tables.TryGetValue(tableName, out entries))
                {
                    this.tables.Add(tableName, entries = new List<MockTableEntry>());
                }

                // verify valid data BEFORE inserting them
                if (entities.Join(entries, ToId, ToId, (u, v) => true).Any())
                {
                    throw new DataServiceRequestException("INSERT: key conflict.");
                }

                if (entities.GroupBy(ToId).Any(id => id.Count() != 1))
                {
                    throw new DataServiceRequestException("INSERT: duplicate keys.");
                }

                // ok, we can insert safely now
                foreach (var entity in entities)
                {
                    var etag = (this.nextETag++).ToString(CultureInfo.InvariantCulture);
                    entity.ETag = etag;
                    entries.Add(
                        new MockTableEntry
                            {
                                PartitionKey = entity.PartitionKey, 
                                RowKey = entity.RowKey, 
                                ETag = etag, 
                                Value = FatEntity.Convert(entity, this.DataSerializer)
                            });
                }
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
        /// <see cref="ITableStorageProvider.Update{T}"/>
        /// <remarks>
        /// </remarks>
        public void Update<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            lock (this.syncRoot)
            {
                List<MockTableEntry> entries;
                if (!this.tables.TryGetValue(tableName, out entries))
                {
                    throw new DataServiceRequestException("UPDATE: table not found.");
                }

                // verify valid data BEFORE updating them
                if (
                    entities.GroupJoin(
                        entries, 
                        ToId, 
                        ToId, 
                        (u, vs) => vs.Count(entry => force || u.ETag == null || entry.ETag == u.ETag)).Any(c => c != 1))
                {
                    throw new DataServiceRequestException("UPDATE: key not found or etag conflict.");
                }

                if (entities.GroupBy(ToId).Any(id => id.Count() != 1))
                {
                    throw new DataServiceRequestException("UPDATE: duplicate keys.");
                }

                // ok, we can update safely now
                foreach (var entity in entities)
                {
                    var etag = (this.nextETag++).ToString(CultureInfo.InvariantCulture);
                    entity.ETag = etag;
                    var index =
                        entries.FindIndex(
                            entry => entry.PartitionKey == entity.PartitionKey && entry.RowKey == entity.RowKey);
                    entries[index] = new MockTableEntry
                        {
                            PartitionKey = entity.PartitionKey, 
                            RowKey = entity.RowKey, 
                            ETag = etag, 
                            Value = FatEntity.Convert(entity, this.DataSerializer)
                        };
                }
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
        /// <see cref="ITableStorageProvider.Update{T}"/>
        /// <remarks>
        /// </remarks>
        public void Upsert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            if (entities == null)
            {
                throw new ArgumentNullException("entities");
            }

            lock (this.syncRoot)
            {
                // deleting all existing entities
                foreach (var g in entities.GroupBy(e => e.PartitionKey))
                {
                    Delete<T>(tableName, g.Key, g.Select(e => e.RowKey));
                }

                // inserting all entities
                this.Insert(tableName, entities);
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Converts to ID.
        /// </summary>
        /// <typeparam name="T">
        /// The type of cloud entity.
        /// </typeparam>
        /// <param name="entity">
        /// The entity. 
        /// </param>
        /// <returns>
        /// A tuple of partition key and row key.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static Tuple<string, string> ToId<T>(CloudEntity<T> entity)
        {
            return Tuple.Create(entity.PartitionKey, entity.RowKey);
        }

        /// <summary>
        /// Converts to ID.
        /// </summary>
        /// <param name="entry">
        /// The entry. 
        /// </param>
        /// <returns>
        /// A tuple of partition key and row key.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static Tuple<string, string> ToId(MockTableEntry entry)
        {
            return Tuple.Create(entry.PartitionKey, entry.RowKey);
        }

        /// <summary>
        /// Gets the internal.
        /// </summary>
        /// <typeparam name="T">
        /// The type of cloud entity.
        /// </typeparam>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="predicate">
        /// The predicate. 
        /// </param>
        /// <returns>
        /// An enumerable of cloud entity of T.
        /// </returns>
        /// <see cref="ITableStorageProvider.Get{T}(string)"/>
        /// <remarks>
        /// </remarks>
        private IEnumerable<CloudEntity<T>> GetInternal<T>(string tableName, Func<MockTableEntry, bool> predicate)
        {
            lock (this.syncRoot)
            {
                if (!this.tables.ContainsKey(tableName))
                {
                    return new List<CloudEntity<T>>();
                }

                return from entry in this.tables[tableName]
                       where predicate(entry)
                       select entry.ToCloudEntity<T>(this.DataSerializer);
            }
        }

        #endregion

        /// <summary>
        /// The mock table entry.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class MockTableEntry
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the E tag.
            /// </summary>
            /// <value> The E tag. </value>
            /// <remarks>
            /// </remarks>
            public string ETag { get; set; }

            /// <summary>
            ///   Gets or sets the partition key.
            /// </summary>
            /// <value> The partition key. </value>
            /// <remarks>
            /// </remarks>
            public string PartitionKey { get; set; }

            /// <summary>
            ///   Gets or sets the row key.
            /// </summary>
            /// <value> The row key. </value>
            /// <remarks>
            /// </remarks>
            public string RowKey { get; set; }

            /// <summary>
            ///   Gets or sets the value.
            /// </summary>
            /// <value> The value. </value>
            /// <remarks>
            /// </remarks>
            public FatEntity Value { get; set; }

            #endregion

            #region Public Methods and Operators

            /// <summary>
            /// Converts to a cloud entity.
            /// </summary>
            /// <typeparam name="T">
            /// The type of cloud entity.
            /// </typeparam>
            /// <param name="serializer">
            /// The serializer. 
            /// </param>
            /// <returns>
            /// The cloud entity of T.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public CloudEntity<T> ToCloudEntity<T>(IDataSerializer serializer)
            {
                return FatEntity.Convert<T>(this.Value, serializer, this.ETag);
            }

            #endregion
        }
    }
}