#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Autofac
{
    using Lokad.Cloud.Storage.Queues;

    using global::Autofac;

    using Lokad.Cloud.Storage.InMemory;
    using Lokad.Cloud.Storage.Tables;

    /// <summary>
    /// IoC Module that provides simple stub in-memory providers without any diagostics attached: - CloudStorageProviders - IBlobStorageProvider - IQueueStorageProvider - ITableStorageProvider
    /// </summary>
    /// <remarks>
    /// </remarks>
    public sealed class StubStorageModule : Module
    {
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
            builder.Register(c => CloudStorage.ForInMemoryStorage().BuildStorageProviders()).OnRelease(
                p => p.QueueStorage.AbandonAll());

            builder.Register(c => new MemoryBlobStorageProvider()).As<IBlobStorageProvider>();

            builder.Register(c => new MemoryQueueStorageProvider()).As<IQueueStorageProvider>().OnRelease(
                p => p.AbandonAll());

            builder.Register(c => new MemoryTableStorageProvider()).As<ITableStorageProvider>();

            builder.Register(c => new NeutralLogStorage { BlobStorage = new MemoryBlobStorageProvider() });
        }

        #endregion
    }
}