#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Blobs
{
    using System;
    using System.Linq;
    using System.Threading;

    using Lokad.Cloud.Storage.Blobs;

    using NUnit.Framework;

    /// <summary>
    /// The memory blob storage tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    [Category("InMemoryStorage")]
    public class MemoryBlobStorageTests : BlobStorageTests
    {
        #region Constants and Fields

        /// <summary>
        /// The container name 1.
        /// </summary>
        private const string ContainerName1 = "container-1";

        /// <summary>
        /// The container name 2.
        /// </summary>
        private const string ContainerName2 = "container-2";

        /// <summary>
        /// The container name 3.
        /// </summary>
        private const string ContainerName3 = "container-3";

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryBlobStorageTests"/> class. 
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryBlobStorageTests()
            : base(CloudStorage.ForInMemoryStorage().BuildStorageProviders())
        {
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Blobses the get created mono thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void BlobsGetCreatedMonoThread()
        {
            const string BlobPrefix = "mockBlobPrefix";
            const string SecondBlobPrefix = "sndBlobPrefix";

            this.BlobStorage.CreateContainerIfNotExist(ContainerName1);
            this.BlobStorage.CreateContainerIfNotExist(ContainerName2);
            this.BlobStorage.CreateContainerIfNotExist(ContainerName3);

            this.BlobStorage.PutBlob(ContainerName1, string.Format("{0}/blob1", BlobPrefix), new DateTime(2009, 08, 27));
            this.BlobStorage.PutBlob(ContainerName1, string.Format("{0}/blob2", BlobPrefix), new DateTime(2009, 08, 28));
            this.BlobStorage.PutBlob(ContainerName1, string.Format("{0}/blob3", BlobPrefix), new DateTime(2009, 08, 29));
            this.BlobStorage.PutBlob(ContainerName2, string.Format("{0}/blob2", BlobPrefix), new DateTime(1984, 07, 06));
            this.BlobStorage.PutBlob(ContainerName1, string.Format("{0}/blob1", SecondBlobPrefix), new DateTime(2009, 08, 30));

            Assert.AreEqual(
                3, 
                this.BlobStorage.ListBlobNames(ContainerName1, BlobPrefix).Count(), 
                "first container with first prefix does not hold 3 blobs");

            Assert.AreEqual(
                1, 
                this.BlobStorage.ListBlobNames(ContainerName2, BlobPrefix).Count(), 
                "second container with first prefix does not hold 1 blobs");

            Assert.AreEqual(
                0, 
                this.BlobStorage.ListBlobNames(ContainerName3, BlobPrefix).Count(), 
                "third container with first prefix does not hold 0 blob");

            Assert.AreEqual(
                1, 
                this.BlobStorage.ListBlobNames(ContainerName1, SecondBlobPrefix).Count(), 
                "first container with second prefix does not hold 1 blobs");
        }

        /// <summary>
        /// Blobses the get created multi thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void BlobsGetCreatedMultiThread()
        {
            const string BlobPrefix = "mockBlobPrefix";

            this.BlobStorage.CreateContainerIfNotExist(ContainerName1);
            this.BlobStorage.CreateContainerIfNotExist(ContainerName2);

            var threads = Enumerable.Range(0, 32).Select(i => new Thread(AddValueToContainer)).ToArray();

            var threadParameters =
                Enumerable.Range(0, 32).Select(
                    i =>
                    i <= 15
                        ? new ThreadParameters("threadId" + i, ContainerName1, this.BlobStorage)
                        : new ThreadParameters("threadId" + i, ContainerName2, this.BlobStorage)).ToArray();

            foreach (var i in Enumerable.Range(0, 32))
            {
                threads[i].Start(threadParameters[i]);
            }

            Thread.Sleep(2000);

            Assert.AreEqual(
                1600, 
                this.BlobStorage.ListBlobNames(ContainerName1, BlobPrefix).Count(), 
                "first container with corresponding prefix does not hold 3 blobs");

            Assert.AreEqual(
                1600, 
                this.BlobStorage.ListBlobNames(ContainerName2, BlobPrefix).Count(), 
                "second container with corresponding prefix does not hold 1 blobs");
        }

        /// <summary>
        /// Tears down.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TearDown]
        public void TearDown()
        {
            this.BlobStorage.DeleteContainerIfExist(ContainerName1);
            this.BlobStorage.DeleteContainerIfExist(ContainerName2);
            this.BlobStorage.DeleteContainerIfExist(ContainerName3);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Adds the value to container.
        /// </summary>
        /// <param name="parameters">
        /// The parameters. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private static void AddValueToContainer(object parameters)
        {
            var castedParameters = parameters as ThreadParameters;
            if (castedParameters == null)
            {
                return;
            }
            
            var random = new Random();
            for (var i = 0; i < 100; i++)
            {
                castedParameters.BlobStorage.PutBlob(
                    castedParameters.ContainerName, 
                    string.Format("mockBlobPrefix{0}/blob{1}", castedParameters.ThreadId, i), 
                    random.NextDouble());
            }
        }

        #endregion

        /// <summary>
        /// The thread parameters.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class ThreadParameters
        {
            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="ThreadParameters"/> class.
            /// </summary>
            /// <param name="threadId">
            /// The thread id. 
            /// </param>
            /// <param name="containerName">
            /// Name of the container. 
            /// </param>
            /// <param name="blobStorage">
            /// The BLOB storage. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public ThreadParameters(string threadId, string containerName, IBlobStorageProvider blobStorage)
            {
                this.BlobStorage = blobStorage;
                this.ThreadId = threadId;
                this.ContainerName = containerName;
            }

            #endregion

            #region Public Properties

            /// <summary>
            ///   Gets the BLOB storage.
            /// </summary>
            /// <value> The BLOB storage. </value>
            /// <remarks>
            /// </remarks>
            public IBlobStorageProvider BlobStorage { get; private set; }

            /// <summary>
            ///   Gets the name of the container.
            /// </summary>
            /// <value> The name of the container. </value>
            /// <remarks>
            /// </remarks>
            public string ContainerName { get; private set; }

            /// <summary>
            ///   Gets the thread id.
            /// </summary>
            /// <value> The thread id. </value>
            /// <remarks>
            /// </remarks>
            public string ThreadId { get; private set; }

            #endregion
        }
    }
}