#region Copyright (c) Lokad 2010-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Tables
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Strong-typed utility wrapper for the <see cref="ITableStorageProvider"/> .
    /// </summary>
    /// <typeparam name="T">
    /// </typeparam>
    /// <remarks>
    /// The purpose of the <c>CloudTable{T}</c> is to provide a strong-typed access to the table storage in the client code. Indeed, the row table storage provider typically let you (potentially) mix different types into a single table.
    /// </remarks>
    public class CloudTable<T>
    {
        #region Constants and Fields

        /// <summary>
        /// The provider.
        /// </summary>
        private readonly ITableStorageProvider provider;

        /// <summary>
        /// The table name.
        /// </summary>
        private readonly string tableName;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudTable{T}"/> class. 
        /// Initializes a new instance of the <see cref="CloudTable&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public CloudTable(ITableStorageProvider provider, string tableName)
        {
            // validating against the Windows Azure rule for table names.
            if (!Regex.Match(tableName, "^[A-Za-z][A-Za-z0-9]{2,62}").Success)
            {
                throw new ArgumentException("Table name is incorrect", "tableName");
            }

            this.provider = provider;
            this.tableName = tableName;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the name of the underlying table.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string Name
        {
            get
            {
                return this.tableName;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Deletes the specified partition key.
        /// </summary>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <param name="rowKeys">
        /// The row keys. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Delete{T}(string, string, IEnumerable{string})"/>
        /// <remarks>
        /// </remarks>
        public void Delete(string partitionKey, IEnumerable<string> rowKeys)
        {
            this.provider.Delete<T>(this.tableName, partitionKey, rowKeys);
        }

        /// <summary>
        /// Deletes the specified partition key.
        /// </summary>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <param name="rowKey">
        /// The row key. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Delete{T}(string, string, IEnumerable{string})"/>
        /// <remarks>
        /// </remarks>
        public void Delete(string partitionKey, string rowKey)
        {
            this.provider.Delete<T>(this.tableName, partitionKey, new[] { rowKey });
        }

        /// <summary>
        /// Gets the specified partition name.
        /// </summary>
        /// <param name="partitionName">
        /// Name of the partition. 
        /// </param>
        /// <param name="rowKey">
        /// The row key. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <seealso cref="ITableStorageProvider.Get{T}(string, string)"/>
        /// <remarks>
        /// </remarks>
        public Maybe<CloudEntity<T>> Get(string partitionName, string rowKey)
        {
            var entity = this.provider.Get<T>(this.tableName, partitionName, new[] { rowKey }).FirstOrDefault();
            return null != entity ? new Maybe<CloudEntity<T>>(entity) : Maybe<CloudEntity<T>>.Empty;
        }

        /// <summary>
        /// Gets this instance.
        /// </summary>
        /// <returns>
        /// </returns>
        /// <seealso cref="ITableStorageProvider.Get{T}(string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get()
        {
            return this.provider.Get<T>(this.tableName);
        }

        /// <summary>
        /// Gets the specified partition key.
        /// </summary>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <seealso cref="ITableStorageProvider.Get{T}(string, string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get(string partitionKey)
        {
            return this.provider.Get<T>(this.tableName, partitionKey);
        }

        /// <summary>
        /// Gets the specified partition key.
        /// </summary>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <param name="startRowKey">
        /// The start row key. 
        /// </param>
        /// <param name="endRowKey">
        /// The end row key. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <seealso cref="ITableStorageProvider.Get{T}(string, string, string, string)"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get(string partitionKey, string startRowKey, string endRowKey)
        {
            return this.provider.Get<T>(this.tableName, partitionKey, startRowKey, endRowKey);
        }

        /// <summary>
        /// Gets the specified partition key.
        /// </summary>
        /// <param name="partitionKey">
        /// The partition key. 
        /// </param>
        /// <param name="rowKeys">
        /// The row keys. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <seealso cref="ITableStorageProvider.Get{T}(string, string, IEnumerable{string})"/>
        /// <remarks>
        /// </remarks>
        public IEnumerable<CloudEntity<T>> Get(string partitionKey, IEnumerable<string> rowKeys)
        {
            return this.provider.Get<T>(this.tableName, partitionKey, rowKeys);
        }

        /// <summary>
        /// Inserts the specified entities.
        /// </summary>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Insert{T}(string, IEnumerable{CloudEntity{T}})"/>
        /// <remarks>
        /// </remarks>
        public void Insert(IEnumerable<CloudEntity<T>> entities)
        {
            this.provider.Insert(this.tableName, entities);
        }

        /// <summary>
        /// Inserts the specified entity.
        /// </summary>
        /// <param name="entity">
        /// The entity. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Insert{T}(string, IEnumerable{CloudEntity{T}})"/>
        /// <remarks>
        /// </remarks>
        public void Insert(CloudEntity<T> entity)
        {
            this.provider.Insert(this.tableName, new[] { entity });
        }

        /// <summary>
        /// Updates the specified entities.
        /// </summary>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Update(IEnumerable<CloudEntity<T>> entities)
        {
            this.provider.Update(this.tableName, entities);
        }

        /// <summary>
        /// Updates the specified entity.
        /// </summary>
        /// <param name="entity">
        /// The entity. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Update(CloudEntity<T> entity)
        {
            this.provider.Update(this.tableName, new[] { entity });
        }

        /// <summary>
        /// Upserts the specified entities.
        /// </summary>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Upsert{T}(string, IEnumerable{CloudEntity{T}})"/>
        /// <remarks>
        /// </remarks>
        public void Upsert(IEnumerable<CloudEntity<T>> entities)
        {
            this.provider.Upsert(this.tableName, entities);
        }

        /// <summary>
        /// Upserts the specified entity.
        /// </summary>
        /// <param name="entity">
        /// The entity. 
        /// </param>
        /// <seealso cref="ITableStorageProvider.Upsert{T}(string, IEnumerable{CloudEntity{T}})"/>
        /// <remarks>
        /// </remarks>
        public void Upsert(CloudEntity<T> entity)
        {
            this.provider.Upsert(this.tableName, new[] { entity });
        }

        #endregion
    }
}