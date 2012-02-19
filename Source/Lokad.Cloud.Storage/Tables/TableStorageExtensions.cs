﻿#region Copyright (c) Lokad 2009-2010

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedMethodReturnValue.Global
namespace Lokad.Cloud.Storage.Tables
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Helper extensions methods for storage providers.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public static class TableStorageExtensions
    {
        #region Public Methods and Operators

        /// <summary>
        /// Deletes a collection of entities.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <remarks>
        /// <para>
        /// The implementation is expected to lazily iterate through all row keys
        ///     and send batch deletion request to the underlying storage.
        /// </para>
        /// <para>
        /// Idempotence of the method is required.
        /// </para>
        /// <para>
        /// The method should not fail if the table does not exist.
        /// </para>
        /// <para>
        /// The call is expected to fail if one or several entities have
        ///                                                                                                                                                                                                  changed remotely in the meantime. Use the overloaded method with the additional
        ///                                                                                                                                                                                                  force parameter to change this behavior if needed.
        /// </para>
        /// </remarks>
        public static void Delete<T>(
            this ITableStorageProvider provider, string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            provider.Delete(tableName, entities, false);
        }

        /// <summary>
        /// Gets the specified cloud entity if it exists.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="partitionName">
        /// Name of the partition. 
        /// </param>
        /// <param name="rowKey">
        /// The row key. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Maybe<CloudEntity<T>> Get<T>(
            this ITableStorageProvider provider, string tableName, string partitionName, string rowKey)
        {
            var entity = provider.Get<T>(tableName, partitionName, new[] { rowKey }).FirstOrDefault();
            return null != entity ? new Maybe<CloudEntity<T>>(entity) : Maybe<CloudEntity<T>>.Empty;
        }

        /// <summary>
        /// Gets a strong typed wrapper around the table storage provider.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudTable<T> GetTable<T>(this ITableStorageProvider provider, string tableName)
        {
            return new CloudTable<T>(provider, tableName);
        }

        /// <summary>
        /// Updates a collection of existing entities into the table storage.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="tableName">
        /// Name of the table. 
        /// </param>
        /// <param name="entities">
        /// The entities. 
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// thrown if the table does not exist
        ///   or an non-existing entity has been encountered.
        /// </exception>
        /// <remarks>
        /// <para>
        /// The call is expected to fail on the first non-existing entity.
        ///     Results are not garanteed if one or several entities do not exist already.
        /// </para>
        /// <para>
        /// The call is also expected to fail if one or several entities have
        ///                                                                                         changed remotely in the meantime. Use the overloaded method with the additional
        ///                                                                                         force parameter to change this behavior if needed.
        /// </para>
        /// <para>
        /// There is no upper limit on the number of entities provided through
        ///                                                                                                                                                     the enumeration. The implementations are expected to lazily iterates
        ///                                                                                                                                                     and to create batch requests as the move forward.
        /// </para>
        /// <para>
        /// Idempotence of the implementation is required.
        /// </para>
        /// </remarks>
        public static void Update<T>(
            this ITableStorageProvider provider, string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            provider.Update(tableName, entities, false);
        }

        #endregion
    }
}