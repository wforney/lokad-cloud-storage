#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Queues
{
    using System;
    using System.Linq;

    using Lokad.Cloud.Storage.Azure;
    using Lokad.Cloud.Storage.Queues;

    using NUnit.Framework;

    /// <summary>
    /// The dev queue storage tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    [Category("DevelopmentStorage")]
    public class DevQueueStorageTests : QueueStorageTests
    {
        #region Constants and Fields

        /// <summary>
        /// The rand.
        /// </summary>
        private static readonly Random Rand = new Random();

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DevQueueStorageTests"/> class. 
        ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public DevQueueStorageTests()
            : base(CloudStorage.ForDevelopmentStorage().BuildStorageProviders())
        {
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Clears the removes overflowing blobs.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ClearRemovesOverflowingBlobs()
        {
            var queueName = "test1-" + Guid.NewGuid().ToString("N");

            // CAUTION: we are now compressing serialization output.
            // hence, we can't just pass an empty array, as it would be compressed at near 100%.
            var data = new byte[80000];
            Rand.NextBytes(data);

            this.QueueStorage.Put(queueName, data);

            // HACK: implicit pattern for listing overflowing messages
            var overflowingCount =
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, queueName).Count();

            Assert.AreEqual(1, overflowingCount, "#A00");

            this.QueueStorage.Clear(queueName);

            overflowingCount =
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, queueName).Count();

            Assert.AreEqual(0, overflowingCount, "#A01");

            this.QueueStorage.DeleteQueue(queueName);
        }

        /// <summary>
        /// Deletes the removes overflowing blobs.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void DeleteRemovesOverflowingBlobs()
        {
            var queueName = "test1-" + Guid.NewGuid().ToString("N");

            // CAUTION: we are now compressing serialization output.
            // hence, we can't just pass an empty array, as it would be compressed at near 100%.
            var data = new byte[80000];
            Rand.NextBytes(data);

            this.QueueStorage.Put(queueName, data);

            // HACK: implicit pattern for listing overflowing messages
            var overflowingCount =
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, queueName).Count();

            Assert.AreEqual(1, overflowingCount, "#A00");

            this.QueueStorage.DeleteQueue(queueName);

            overflowingCount =
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, queueName).Count();

            Assert.AreEqual(0, overflowingCount, "#A01");
        }

        /// <summary>
        /// Persists the restore overflowing.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PersistRestoreOverflowing()
        {
            const string StoreName = "TestStore";

            // CAUTION: we are now compressing serialization output.
            // hence, we can't just pass an empty array, as it would be compressed at near 100%.
            var data = new byte[80000];
            Rand.NextBytes(data);

            // clean up
            this.QueueStorage.DeleteQueue(this.QueueName);
            foreach (var skey in this.QueueStorage.ListPersisted(StoreName))
            {
                this.QueueStorage.DeletePersisted(StoreName, skey);
            }

            // put
            this.QueueStorage.Put(this.QueueName, data);

            Assert.AreEqual(
                1, 
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, this.QueueName).Count(), 
                "#A01");

            // get
            var retrieved = this.QueueStorage.Get<byte[]>(this.QueueName, 1).First();

            // persist
            this.QueueStorage.Persist(retrieved, StoreName, "manual test");

            Assert.AreEqual(
                1, 
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, this.QueueName).Count(), 
                "#A02");

            // abandon should fail (since not invisible anymore)
            Assert.IsFalse(this.QueueStorage.Abandon(retrieved), "#A03");

            // list persisted message
            var key = this.QueueStorage.ListPersisted(StoreName).Single();

            // get persisted message
            var persisted = this.QueueStorage.GetPersisted(StoreName, key);
            Assert.IsTrue(persisted.HasValue, "#A04");
            Assert.IsTrue(persisted.Value.DataXml.HasValue, "#A05");

            // delete persisted message
            this.QueueStorage.DeletePersisted(StoreName, key);

            Assert.AreEqual(
                0, 
                this.BlobStorage.ListBlobNames(QueueStorageProvider.OverflowingMessagesContainerName, this.QueueName).Count(), 
                "#A06");

            // list no longer contains key
            Assert.IsFalse(this.QueueStorage.ListPersisted(StoreName).Any(), "#A07");
        }

        /// <summary>
        /// Puts the get delete overflowing.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutGetDeleteOverflowing()
        {
            // 20k chosen so that it doesn't fit into the queue.
            var message = new MyMessage { MyBuffer = new byte[80000] };

            // fill buffer with random content
            Rand.NextBytes(message.MyBuffer);

            this.QueueStorage.Clear(this.QueueName);

            this.QueueStorage.Put(this.QueueName, message);
            var retrieved = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();

            Assert.AreEqual(message.MyGuid, retrieved.MyGuid, "#A01");
            CollectionAssert.AreEquivalent(message.MyBuffer, retrieved.MyBuffer, "#A02");

            for (var i = 0; i < message.MyBuffer.Length; i++)
            {
                Assert.AreEqual(message.MyBuffer[i], retrieved.MyBuffer[i], "#A02-" + i);
            }

            this.QueueStorage.Delete(retrieved);
        }

        /// <summary>
        /// Queues the latency.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void QueueLatency()
        {
            Assert.IsFalse(this.QueueStorage.GetApproximateLatency(this.QueueName).HasValue);

            this.QueueStorage.Put(this.QueueName, 100);

            var latency = this.QueueStorage.GetApproximateLatency(this.QueueName);
            Assert.IsTrue(latency.HasValue);
            Assert.IsTrue(latency.Value >= TimeSpan.Zero && latency.Value < TimeSpan.FromMinutes(10));

            this.QueueStorage.Delete(100);
        }

        #endregion
    }
}