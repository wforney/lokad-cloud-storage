#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Tables;

    /// <summary>
    /// Storage providers and runtime providers.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class CloudStorageProviders
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudStorageProviders"/> class.
        /// </summary>
        /// <param name="blobStorage">
        /// The BLOB storage. 
        /// </param>
        /// <param name="queueStorage">
        /// The queue storage. 
        /// </param>
        /// <param name="tableStorage">
        /// The table storage. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public CloudStorageProviders(
            IBlobStorageProvider blobStorage, IQueueStorageProvider queueStorage, ITableStorageProvider tableStorage)
        {
            this.BlobStorage = blobStorage;
            this.QueueStorage = queueStorage;
            this.TableStorage = tableStorage;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets Blob Storage Abstraction.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public IBlobStorageProvider BlobStorage { get; private set; }

        /// <summary>
        ///   Gets Queue Storage Abstraction.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public IQueueStorageProvider QueueStorage { get; private set; }

        /// <summary>
        ///   Gets Table Storage Abstraction.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public ITableStorageProvider TableStorage { get; private set; }

        #endregion
    }
}