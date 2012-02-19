#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using Lokad.Cloud.Storage.InMemory;
    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Tables;

    /// <summary>
    /// The in memory storage builder.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal sealed class InMemoryStorageBuilder : CloudStorage.CloudStorageBuilder
    {
        #region Public Methods and Operators

        /// <summary>
        /// Builds the BLOB storage.
        /// </summary>
        /// <returns>
        /// The BLOB storage.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override IBlobStorageProvider BuildBlobStorage()
        {
            return new MemoryBlobStorageProvider { DefaultSerializer = this.DataSerializer };
        }

        /// <summary>
        /// Builds the queue storage.
        /// </summary>
        /// <returns>
        /// The queue storage.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override IQueueStorageProvider BuildQueueStorage()
        {
            return new MemoryQueueStorageProvider { DefaultSerializer = this.DataSerializer };
        }

        /// <summary>
        /// Builds the table storage.
        /// </summary>
        /// <returns>
        /// The table storage.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override ITableStorageProvider BuildTableStorage()
        {
            return new MemoryTableStorageProvider { DataSerializer = this.DataSerializer };
        }

        #endregion
    }
}