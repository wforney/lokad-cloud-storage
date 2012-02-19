#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.ComponentModel;

    using Lokad.Cloud.Storage.Instrumentation;
    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Tables;

    using Microsoft.WindowsAzure;

    /// <summary>
    /// Helper class to get access to cloud storage providers.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public static class CloudStorage
    {
        #region Public Methods and Operators

        /// <summary>
        /// Fors the azure account.
        /// </summary>
        /// <param name="storageAccount">
        /// The storage account. 
        /// </param>
        /// <returns>
        /// The cloud storage builder.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudStorageBuilder ForAzureAccount(CloudStorageAccount storageAccount)
        {
            return new AzureCloudStorageBuilder(storageAccount);
        }

        /// <summary>
        /// Fors the azure account and key.
        /// </summary>
        /// <param name="accountName">
        /// Name of the account. 
        /// </param>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="useHttps">
        /// if set to <c>true</c> [use HTTPS]. 
        /// </param>
        /// <returns>
        /// The cloud storage builder.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudStorageBuilder ForAzureAccountAndKey(string accountName, string key, bool useHttps = true)
        {
            return
                new AzureCloudStorageBuilder(
                    new CloudStorageAccount(new StorageCredentialsAccountAndKey(accountName, key), useHttps));
        }

        /// <summary>
        /// Fors the azure connection string.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string. 
        /// </param>
        /// <returns>
        /// The cloud storage builder.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudStorageBuilder ForAzureConnectionString(string connectionString)
        {
            CloudStorageAccount storageAccount;
            if (!CloudStorageAccount.TryParse(connectionString, out storageAccount))
            {
                throw new InvalidOperationException("Failed to get valid connection string");
            }

            return new AzureCloudStorageBuilder(storageAccount);
        }

        /// <summary>
        /// Fors the development storage.
        /// </summary>
        /// <returns>
        /// The cloud storage builder.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudStorageBuilder ForDevelopmentStorage()
        {
            return new AzureCloudStorageBuilder(CloudStorageAccount.DevelopmentStorageAccount);
        }

        /// <summary>
        /// Fors the in memory storage.
        /// </summary>
        /// <returns>
        /// The cloud storage builder.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudStorageBuilder ForInMemoryStorage()
        {
            return new InMemoryStorageBuilder();
        }

        #endregion

        /// <summary>
        /// The cloud storage builder.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract class CloudStorageBuilder
        {
            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="CloudStorageBuilder"/> class. 
            ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            /// <remarks>
            /// </remarks>
            protected CloudStorageBuilder()
            {
                // defaults
                this.DataSerializer = new CloudFormatter();
            }

            #endregion

            #region Properties

            /// <summary>
            ///   Gets the data serializer.
            /// </summary>
            /// <remarks>
            ///   Can not be null
            /// </remarks>
            protected IDataSerializer DataSerializer { get; private set; }

            /// <summary>
            ///   Gets or sets the observer.
            /// </summary>
            /// <value> The observer. </value>
            /// <remarks>
            ///   Can be null if not needed
            /// </remarks>
            protected IStorageObserver Observer { get; set; }

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
            public abstract IBlobStorageProvider BuildBlobStorage();

            /// <summary>
            /// Builds the queue storage.
            /// </summary>
            /// <returns>
            /// The queue storage.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public abstract IQueueStorageProvider BuildQueueStorage();

            /// <summary>
            /// Builds the storage providers.
            /// </summary>
            /// <returns>
            /// The storage providers.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public CloudStorageProviders BuildStorageProviders()
            {
                var blobStorage = this.BuildBlobStorage();
                var queueStorage = this.BuildQueueStorage();
                var tableStorage = this.BuildTableStorage();

                return new CloudStorageProviders(blobStorage, queueStorage, tableStorage);
            }

            /// <summary>
            /// Builds the table storage.
            /// </summary>
            /// <returns>
            /// The table storage.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public abstract ITableStorageProvider BuildTableStorage();

            /// <summary>
            /// Replace the default data serializer with a custom implementation
            /// </summary>
            /// <param name="dataSerializer">
            /// The data serializer. 
            /// </param>
            /// <returns>
            /// The cloud storage builder.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public CloudStorageBuilder WithDataSerializer(IDataSerializer dataSerializer)
            {
                this.DataSerializer = dataSerializer;
                return this;
            }

            /// <summary>
            /// Optionally provide a storage event observer, e.g. a <see cref="StorageObserverSubject"/> .
            /// </summary>
            /// <param name="observer">
            /// The observer. 
            /// </param>
            /// <returns>
            /// The cloud storage builder.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public CloudStorageBuilder WithObserver(IStorageObserver observer)
            {
                this.Observer = observer;
                return this;
            }

            /// <summary>
            /// Optionally provide a set of observers, will use a <see cref="StorageObserverSubject"/> internally.
            /// </summary>
            /// <param name="observers">
            /// The observers. 
            /// </param>
            /// <returns>
            /// The cloud storage builder.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public CloudStorageBuilder WithObservers(params IObserver<IStorageEvent>[] observers)
            {
                this.Observer = new StorageObserverSubject(observers);
                return this;
            }

            #endregion
        }
    }
}