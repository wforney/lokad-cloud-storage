#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.Net;

    using Lokad.Cloud.Storage.Azure;
    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Tables;

    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.StorageClient;

    using RetryPolicies = Lokad.Cloud.Storage.Azure.RetryPolicies;

    /// <summary>
    /// The azure cloud storage builder.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal sealed class AzureCloudStorageBuilder : CloudStorage.CloudStorageBuilder
    {
        #region Constants and Fields

        /// <summary>
        /// The storage account.
        /// </summary>
        private readonly CloudStorageAccount storageAccount;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureCloudStorageBuilder"/> class.
        /// </summary>
        /// <param name="storageAccount">
        /// The storage account. 
        /// </param>
        /// <remarks>
        /// </remarks>
        internal AzureCloudStorageBuilder(CloudStorageAccount storageAccount)
        {
            this.storageAccount = storageAccount;

            // http://blogs.msdn.com/b/windowsazurestorage/archive/2010/06/25/nagle-s-algorithm-is-not-friendly-towards-small-requests.aspx
            ServicePointManager.FindServicePoint(storageAccount.TableEndpoint).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(storageAccount.QueueEndpoint).UseNagleAlgorithm = false;
        }

        #endregion

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
            return new BlobStorageProvider(this.BlobClient(), this.DataSerializer, this.Observer);
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
            return new QueueStorageProvider(
                this.QueueClient(), this.BuildBlobStorage(), this.DataSerializer, this.Observer);
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
            return new TableStorageProvider(this.TableClient(), this.DataSerializer, this.Observer);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Gets the BLOB client.
        /// </summary>
        /// <returns>
        /// The BLOB client.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private CloudBlobClient BlobClient()
        {
            var policies = new RetryPolicies(this.Observer);
            var blobClient = this.storageAccount.CreateCloudBlobClient();
            blobClient.RetryPolicy = policies.ForAzureStorageClient;
            return blobClient;
        }

        /// <summary>
        /// Gets the queue client.
        /// </summary>
        /// <returns>
        /// The queue client.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private CloudQueueClient QueueClient()
        {
            var policies = new RetryPolicies(this.Observer);
            var queueClient = this.storageAccount.CreateCloudQueueClient();
            queueClient.RetryPolicy = policies.ForAzureStorageClient;
            queueClient.Timeout = TimeSpan.FromSeconds(300);
            return queueClient;
        }

        /// <summary>
        /// Gets the table client.
        /// </summary>
        /// <returns>
        /// The table client.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private CloudTableClient TableClient()
        {
            var policies = new RetryPolicies(this.Observer);
            var tableClient = this.storageAccount.CreateCloudTableClient();
            tableClient.RetryPolicy = policies.ForAzureStorageClient;
            return tableClient;
        }

        #endregion
    }
}