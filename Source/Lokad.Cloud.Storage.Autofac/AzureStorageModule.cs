#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Autofac
{
    using System.Net;

    using global::Autofac;

    using Lokad.Cloud.Storage.Instrumentation;

    using Microsoft.WindowsAzure;

    /// <summary>
    /// IoC Module that provides storage providers linked to Windows Azure storage: - CloudStorageProviders - IBlobStorageProvider - IQueueStorageProvider - ITableStorageProvider Expected external registrations: - Microsoft.WindowsAzure.CloudStorageAccount
    /// </summary>
    /// <remarks>
    /// </remarks>
    public sealed class AzureStorageModule : Module
    {
        #region Constants and Fields

        /// <summary>
        /// The account.
        /// </summary>
        private readonly CloudStorageAccount account;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="AzureStorageModule" /> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public AzureStorageModule()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageModule"/> class.
        /// </summary>
        /// <param name="account">
        /// The account. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public AzureStorageModule(CloudStorageAccount account)
        {
            this.account = this.Patch(account);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Override to add registrations to the container.
        /// </summary>
        /// <param name="builder">
        /// The builder through which components can be registered. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register(
                c =>
                CloudStorage.ForAzureAccount(this.account ?? this.Patch(c.Resolve<CloudStorageAccount>())).
                    WithDataSerializer(c.Resolve<IDataSerializer>()).WithObserver(c.ResolveOptional<IStorageObserver>())
                    .BuildStorageProviders()).OnRelease(p => p.QueueStorage.AbandonAll());

            builder.Register(
                c =>
                CloudStorage.ForAzureAccount(this.account ?? this.Patch(c.Resolve<CloudStorageAccount>())).
                    WithDataSerializer(c.Resolve<IDataSerializer>()).WithObserver(c.ResolveOptional<IStorageObserver>())
                    .BuildBlobStorage());

            builder.Register(
                c =>
                CloudStorage.ForAzureAccount(this.account ?? this.Patch(c.Resolve<CloudStorageAccount>())).
                    WithDataSerializer(c.Resolve<IDataSerializer>()).WithObserver(c.ResolveOptional<IStorageObserver>())
                    .BuildQueueStorage()).OnRelease(p => p.AbandonAll());

            builder.Register(
                c =>
                CloudStorage.ForAzureAccount(this.account ?? this.Patch(c.Resolve<CloudStorageAccount>())).
                    WithDataSerializer(c.Resolve<IDataSerializer>()).WithObserver(c.ResolveOptional<IStorageObserver>())
                    .BuildTableStorage());

            builder.Register(
                c =>
                new NeutralLogStorage
                    {
                        BlobStorage =
                            CloudStorage.ForAzureAccount(this.account ?? this.Patch(c.Resolve<CloudStorageAccount>())).
                            WithDataSerializer(new CloudFormatter()).BuildBlobStorage()
                    });
        }

        /// <summary>
        /// Patches the specified account.
        /// </summary>
        /// <param name="account">
        /// The account. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private CloudStorageAccount Patch(CloudStorageAccount account)
        {
            ServicePointManager.FindServicePoint(account.BlobEndpoint).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(account.TableEndpoint).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(account.QueueEndpoint).UseNagleAlgorithm = false;
            return account;
        }

        #endregion
    }
}