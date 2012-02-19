#region Copyright (c) Lokad 2010

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Tables
{
    using System;

    /// <summary>
    /// Entity to be stored by the <see cref="ITableStorageProvider"/> .
    /// </summary>
    /// <typeparam name="T">
    /// Type of the value carried by the entity. 
    /// </typeparam>
    /// <remarks>
    /// Once serialized the <c>CloudEntity.Value</c> should weight less than 720KB to be compatible with Table Storage limitations on entities.
    /// </remarks>
    public class CloudEntity<T>
    {
        #region Public Properties

        /// <summary>
        ///   ETag. Indicates changes. Populated by the Table Storage.
        /// </summary>
        /// <value> The E tag. </value>
        /// <remarks>
        /// </remarks>
        public string ETag { get; set; }

        /// <summary>
        ///   Indexed system property.
        /// </summary>
        /// <value> The partition key. </value>
        /// <remarks>
        /// </remarks>
        public string PartitionKey { get; set; }

        /// <summary>
        ///   Indexed system property.
        /// </summary>
        /// <value> The row key. </value>
        /// <remarks>
        /// </remarks>
        public string RowKey { get; set; }

        /// <summary>
        ///   Flag indicating last update. Populated by the Table Storage.
        /// </summary>
        /// <value> The timestamp. </value>
        /// <remarks>
        /// </remarks>
        public DateTime Timestamp { get; set; }

        /// <summary>
        ///   Value carried by the entity.
        /// </summary>
        /// <value> The value. </value>
        /// <remarks>
        /// </remarks>
        public T Value { get; set; }

        #endregion
    }
}