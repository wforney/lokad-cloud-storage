#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

 // TODO: refactor tests so that containers do not have to be created each time.
namespace Lokad.Cloud.Storage.Test.Blobs
{
    // ReSharper disable InconsistentNaming
    using System;
    using System.Linq;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Shared.Monads;
    using Lokad.Cloud.Storage.Test.Shared;

    using NUnit.Framework;

    /// <summary>
    /// The blob storage tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public abstract class BlobStorageTests
    {
        #region Constants and Fields

        /// <summary>
        /// The blob storage.
        /// </summary>
        protected readonly IBlobStorageProvider BlobStorage;

        /// <summary>
        /// The blob name.
        /// </summary>
        private const string BlobName = "myprefix/myblob";

        /// <summary>
        /// The container name.
        /// </summary>
        private const string ContainerName = "tests-blobstorageprovider-mycontainer";

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorageTests"/> class.
        /// </summary>
        /// <param name="storage">
        /// The storage. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected BlobStorageTests(CloudStorageProviders storage)
        {
            this.BlobStorage = storage.BlobStorage;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// BLOBs the has etag.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void BlobHasEtag()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);
            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            Assert.IsNotNull(etag, "#A00");
        }

        /// <summary>
        /// Determines whether this instance [can acquire BLOB lease].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanAcquireBlobLease()
        {
            var blobName = this.CreateNewBlob();
            var result = this.BlobStorage.TryAcquireLease(ContainerName, blobName);
            Assert.IsTrue(result.IsSuccess);
            Assert.IsNotNullOrEmpty(result.Value);
        }

        /// <summary>
        /// Determines whether this instance [can list containers].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanListContainers()
        {
            Assert.IsTrue(this.BlobStorage.ListContainers().Contains(ContainerName));
            Assert.IsTrue(this.BlobStorage.ListContainers(ContainerName.Substring(0, 5)).Contains(ContainerName));
            Assert.IsFalse(this.BlobStorage.ListContainers("another-prefix").Contains(ContainerName));
        }

        /// <summary>
        /// Determines whether this instance [can not acquire BLOB lease on locked BLOB].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanNotAcquireBlobLeaseOnLockedBlob()
        {
            var blobName = this.CreateNewBlob();
            var result = this.BlobStorage.TryAcquireLease(ContainerName, blobName);
            Assert.IsTrue(result.IsSuccess);
            Assert.IsNotNullOrEmpty(result.Value);

            // Second trial should fail
            result = this.BlobStorage.TryAcquireLease(ContainerName, blobName);
            Assert.IsFalse(result.IsSuccess);
            Assert.AreEqual("Conflict", result.Error);
        }

        /// <summary>
        /// Determines whether this instance [can not release locked BLOB without matching lease id].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanNotReleaseLockedBlobWithoutMatchingLeaseId()
        {
            var blobName = this.CreateNewBlob();
            this.BlobStorage.TryAcquireLease(ContainerName, blobName);
            Assert.IsFalse(
                this.BlobStorage.TryReleaseLease(ContainerName, blobName, Guid.NewGuid().ToString("N")).IsSuccess);
        }

        /// <summary>
        /// Determines whether this instance [can not release unleased BLOB].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanNotReleaseUnleasedBlob()
        {
            var blobName = this.CreateNewBlob();
            Assert.IsFalse(
                this.BlobStorage.TryReleaseLease(ContainerName, blobName, Guid.NewGuid().ToString("N")).IsSuccess);
        }

        /// <summary>
        /// Determines whether this instance [can release locked BLOB with matching lease id].
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CanReleaseLockedBlobWithMatchingLeaseId()
        {
            var blobName = this.CreateNewBlob();
            var lease = this.BlobStorage.TryAcquireLease(ContainerName, blobName);
            Assert.IsTrue(this.BlobStorage.TryReleaseLease(ContainerName, blobName, lease.Value).IsSuccess);
        }

        /// <summary>
        /// Creates the put get range delete.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CreatePutGetRangeDelete()
        {
            var privateContainerName = "test-" + Guid.NewGuid().ToString("N");

            this.BlobStorage.CreateContainerIfNotExist(privateContainerName);

            var blobNames = new[] { BlobName + "-0", BlobName + "-1", BlobName + "-2", BlobName + "-3" };

            var inputBlobs = new[] { new MyBlob(), new MyBlob(), new MyBlob(), new MyBlob() };

            for (var i = 0; i < blobNames.Length; i++)
            {
                this.BlobStorage.PutBlob(privateContainerName, blobNames[i], inputBlobs[i]);
            }

            string[] allEtags;
            var allBlobs = this.BlobStorage.GetBlobRange<MyBlob>(privateContainerName, blobNames, out allEtags);

            Assert.AreEqual(blobNames.Length, allEtags.Length, "Wrong etags array length");
            Assert.AreEqual(blobNames.Length, allBlobs.Length, "Wrong blobs array length");

            for (var i = 0; i < allBlobs.Length; i++)
            {
                Assert.IsNotNull(allEtags[i], "Etag should have been set");
                Assert.IsTrue(allBlobs[i].HasValue, "Blob should have content");
                Assert.AreEqual(inputBlobs[i].MyGuid, allBlobs[i].Value.MyGuid, "Wrong blob content");
            }

            // Test missing blob
            var wrongBlobNames = new string[blobNames.Length + 1];
            Array.Copy(blobNames, wrongBlobNames, blobNames.Length);
            wrongBlobNames[wrongBlobNames.Length - 1] = "inexistent-blob";

            allBlobs = this.BlobStorage.GetBlobRange<MyBlob>(privateContainerName, wrongBlobNames, out allEtags);

            Assert.AreEqual(wrongBlobNames.Length, allEtags.Length, "Wrong etags array length");
            Assert.AreEqual(wrongBlobNames.Length, allBlobs.Length, "Wrong blobs array length");

            for (var i = 0; i < allBlobs.Length - 1; i++)
            {
                Assert.IsNotNull(allEtags[i], "Etag should have been set");
                Assert.IsTrue(allBlobs[i].HasValue, "Blob should have content");
                Assert.AreEqual(inputBlobs[i].MyGuid, allBlobs[i].Value.MyGuid, "Wrong blob content");
            }

            Assert.IsNull(allEtags[allEtags.Length - 1], "Etag should be null");
            Assert.IsFalse(allBlobs[allBlobs.Length - 1].HasValue, "Blob should not have a value");

            this.BlobStorage.DeleteContainerIfExist(privateContainerName);
        }

        /// <summary>
        /// Etags the changes only with blog change.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void EtagChangesOnlyWithBlogChange()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);
            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            var newEtag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            Assert.AreEqual(etag, newEtag, "#A00");
        }

        /// <summary>
        /// Etags the changes with blog change.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void EtagChangesWithBlogChange()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);
            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);
            var newEtag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            Assert.AreNotEqual(etag, newEtag, "#A00.");
        }

        /// <summary>
        /// Gets the and delete.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void GetAndDelete()
        {
            this.BlobStorage.DeleteBlobIfExist(ContainerName, BlobName);
            Assert.IsFalse(this.BlobStorage.GetBlob<int>(ContainerName, BlobName).HasValue, "#A00");
        }

        /// <summary>
        /// Gets the BLOB if not modified no change no retrieval.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void GetBlobIfNotModifiedNoChangeNoRetrieval()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);
            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);

            string newEtag;
            var output = this.BlobStorage.GetBlobIfModified<MyBlob>(ContainerName, BlobName, etag, out newEtag);

            Assert.IsNull(newEtag, "#A00");
            Assert.IsFalse(output.HasValue, "#A01");
        }

        /// <summary>
        /// Gets the BLOB if not modified with type mistmatch.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void GetBlobIfNotModifiedWithTypeMistmatch()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1); // pushing Int32

            string newEtag; // pulling MyBlob
            var output = this.BlobStorage.GetBlobIfModified<MyBlob>(ContainerName, BlobName, "dummy", out newEtag);
            Assert.IsFalse(output.HasValue);
        }

        /// <summary>
        /// Gets the BLOB XML.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void GetBlobXml()
        {
            var data = new MyBlob();
            this.BlobStorage.PutBlob(ContainerName, BlobName, data, true);

            string ignored;
            var blob = this.BlobStorage.GetBlobXml(ContainerName, BlobName, out ignored);
            this.BlobStorage.DeleteBlobIfExist(ContainerName, BlobName);

            Assert.IsTrue(blob.HasValue);
            var xml = blob.Value;
            var property = xml.Elements().Single();
            Assert.AreEqual(data.MyGuid, new Guid(property.Value));
        }

        /// <summary>
        /// Lists the BLOB locations.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ListBlobLocations()
        {
            var prefix = Guid.NewGuid().ToString("N");

            var prefixed = Range.Array(10).Select(i => prefix + Guid.NewGuid().ToString("N")).ToArray();
            var unprefixed = Range.Array(13).Select(i => Guid.NewGuid().ToString("N")).ToArray();

            foreach (var n in prefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            foreach (var n in unprefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            var list = this.BlobStorage.ListBlobLocations(ContainerName, prefix).ToArray();

            Assert.AreEqual(prefixed.Length, list.Length, "#A00");

            foreach (var n in list)
            {
                Assert.AreEqual(ContainerName, n.ContainerName);
                Assert.IsTrue(prefixed.Contains(n.Path), "#A01");
                Assert.IsFalse(unprefixed.Contains(n.Path), "#A02");
            }
        }

        /// <summary>
        /// Lists the BLOB names.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ListBlobNames()
        {
            var prefix = Guid.NewGuid().ToString("N");

            var prefixed = Range.Array(10).Select(i => prefix + Guid.NewGuid().ToString("N")).ToArray();
            var unprefixed = Range.Array(13).Select(i => Guid.NewGuid().ToString("N")).ToArray();

            foreach (var n in prefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            foreach (var n in unprefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            var list = this.BlobStorage.ListBlobNames(ContainerName, prefix).ToArray();

            Assert.AreEqual(prefixed.Length, list.Length, "#A00");

            foreach (var n in list)
            {
                Assert.IsTrue(prefixed.Contains(n), "#A01");
                Assert.IsFalse(unprefixed.Contains(n), "#A02");
            }
        }

        /// <summary>
        /// Lists the blobs.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ListBlobs()
        {
            var prefix = Guid.NewGuid().ToString("N");

            var prefixed = Range.Array(10).Select(i => prefix + Guid.NewGuid().ToString("N")).ToArray();
            var unprefixed = Range.Array(13).Select(i => Guid.NewGuid().ToString("N")).ToArray();

            foreach (var n in prefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            foreach (var n in unprefixed)
            {
                this.BlobStorage.PutBlob(ContainerName, n, n);
            }

            var list = this.BlobStorage.ListBlobs<string>(ContainerName, prefix).ToArray();

            Assert.AreEqual(prefixed.Length, list.Length, "#A00");

            foreach (var n in list)
            {
                Assert.IsTrue(prefixed.Contains(n), "#A01");
                Assert.IsFalse(unprefixed.Contains(n), "#A02");
            }
        }

        /// <summary>
        /// Missings the BLOB has no etag.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void MissingBlobHasNoEtag()
        {
            this.BlobStorage.DeleteBlobIfExist(ContainerName, BlobName);
            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            Assert.IsNull(etag, "#A00");
        }

        /// <summary>
        /// Nullables the type_ default.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void NullableType_Default()
        {
            var privateContainerName = "test-" + Guid.NewGuid().ToString("N");

            this.BlobStorage.CreateContainerIfNotExist(privateContainerName);

            int? value1 = 10;
            int? value2 = null;

            this.BlobStorage.PutBlob(privateContainerName, "test1", value1);
            this.BlobStorage.PutBlob(privateContainerName, "test2", value1);

            var output1 = this.BlobStorage.GetBlob<int?>(privateContainerName, "test1");
            var output2 = this.BlobStorage.GetBlob<int?>(privateContainerName, "test2");

            Assert.AreEqual(value1.Value, output1.Value);
            Assert.IsFalse(value2.HasValue);

            this.BlobStorage.DeleteContainerIfExist(privateContainerName);
        }

        /// <summary>
        /// Puts the BLOB enforce matching etag.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutBlobEnforceMatchingEtag()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);

            var etag = this.BlobStorage.GetBlobEtag(ContainerName, BlobName);
            var isUpdated = this.BlobStorage.PutBlob(ContainerName, BlobName, 2, Guid.NewGuid().ToString());

            Assert.IsTrue(!isUpdated, "#A00 Blob shouldn't be updated if etag is not matching");

            isUpdated = this.BlobStorage.PutBlob(ContainerName, BlobName, 3, etag);
            Assert.IsTrue(isUpdated, "#A01 Blob should have been updated");
        }

        /// <summary>
        /// Puts the BLOB enforce no overwrite.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutBlobEnforceNoOverwrite()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);

            string etag;
            var isSaved = this.BlobStorage.PutBlob(ContainerName, BlobName, 6, false, out etag);
            Assert.IsFalse(isSaved, "#A00");
            Assert.IsNull(etag, "#A01");

            Assert.IsTrue(this.BlobStorage.GetBlob<int>(ContainerName, BlobName).HasValue, "#A02");
            Assert.AreEqual(1, this.BlobStorage.GetBlob<int>(ContainerName, BlobName).Value, "#A03");
        }

        /// <summary>
        /// Puts the BLOB enforce overwrite.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutBlobEnforceOverwrite()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 1);

            string etag;
            var isSaved = this.BlobStorage.PutBlob(ContainerName, BlobName, 6, true, out etag);
            Assert.IsTrue(isSaved, "#A00");
            Assert.IsNotNull(etag, "#A01");

            var maybe = this.BlobStorage.GetBlob<int>(ContainerName, BlobName);
            Assert.IsTrue(maybe.HasValue, "#A02");
            Assert.AreEqual(6, maybe.Value, "#A03");
        }

        /// <summary>
        /// The purpose of this test is to further check MD5 behavior below and above the 32MB threshold (plus the below/above 4MB too).
        /// </summary>
        /// <remarks>
        /// </remarks>
        public void PutBlobWithGrowingSizes()
        {
            var rand = new Random(0);
            foreach (var i in new[] { /*1, 2, 4,*/ 25, 40 })
            {
                var buffer = new byte[(i * 1000000)];
                rand.NextBytes(buffer);

                this.BlobStorage.PutBlob(ContainerName, BlobName, buffer);
                var maybe = this.BlobStorage.GetBlob<byte[]>(ContainerName, BlobName);

                Assert.IsTrue(maybe.HasValue);

                for (var j = 0; j < buffer.Length; j++)
                {
                    Assert.AreEqual(buffer[j], maybe.Value[j]);
                }
            }
        }

        /// <summary>
        /// Setups this instance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestFixtureSetUp]
        public void Setup()
        {
            this.BlobStorage.CreateContainerIfNotExist(ContainerName);
            this.BlobStorage.DeleteBlobIfExist(ContainerName, BlobName);
        }

        /// <summary>
        /// Upserts the block or skip no stress.
        /// </summary>
        /// <remarks>
        /// This test does not check the behavior in case of concurrency stress.
        /// </remarks>
        [Test]
        public void UpsertBlockOrSkipNoStress()
        {
            var blobName = "test" + Guid.NewGuid().ToString("N");
            Assert.IsFalse(this.BlobStorage.GetBlob<int>(ContainerName, blobName).HasValue);

            int inserted = 0, updated = 10;

            // ReSharper disable AccessToModifiedClosure

            // skip insert
            Assert.IsFalse(
                this.BlobStorage.UpsertBlobOrSkip(ContainerName, blobName, () => Maybe<int>.Empty, x => ++updated).HasValue);
            Assert.AreEqual(0, inserted);
            Assert.AreEqual(10, updated);
            Assert.IsFalse(this.BlobStorage.GetBlob<int>(ContainerName, blobName).HasValue);

            // do insert
            Assert.IsTrue(
                this.BlobStorage.UpsertBlobOrSkip<int>(ContainerName, blobName, () => ++inserted, x => ++updated).HasValue);
            Assert.AreEqual(1, inserted);
            Assert.AreEqual(10, updated);
            Assert.AreEqual(1, this.BlobStorage.GetBlob<int>(ContainerName, blobName).Value);

            // skip update
            Assert.IsFalse(
                this.BlobStorage.UpsertBlobOrSkip<int>(ContainerName, blobName, () => ++inserted, x => Maybe<int>.Empty).HasValue);
            Assert.AreEqual(1, inserted);
            Assert.AreEqual(10, updated);
            Assert.AreEqual(1, this.BlobStorage.GetBlob<int>(ContainerName, blobName).Value);

            // do update
            Assert.IsTrue(
                this.BlobStorage.UpsertBlobOrSkip<int>(ContainerName, blobName, () => ++inserted, x => ++updated).HasValue);
            Assert.AreEqual(1, inserted);
            Assert.AreEqual(11, updated);
            Assert.AreEqual(11, this.BlobStorage.GetBlob<int>(ContainerName, blobName).Value);

            // cleanup
            this.BlobStorage.DeleteBlobIfExist(ContainerName, blobName);

            // ReSharper restore AccessToModifiedClosure
        }

        /// <summary>
        /// Upserts the block or skip with stress.
        /// </summary>
        /// <remarks>
        /// Loose check of the behavior under concurrency stress.
        /// </remarks>
        [Test]
        public void UpsertBlockOrSkipWithStress()
        {
            this.BlobStorage.PutBlob(ContainerName, BlobName, 0);

            var array = new Maybe<int>[8];
            array =
                array.AsParallel().Select(
                    k => this.BlobStorage.UpsertBlobOrSkip<int>(ContainerName, BlobName, () => 1, i => i + 1)).ToArray();

            Assert.IsFalse(array.Any(x => !x.HasValue), "No skips");

            var sorted = array.Select(m => m.Value).OrderBy(i => i).ToArray();

            for (var i = 0; i < array.Length; i++)
            {
                Assert.AreEqual(i + 1, sorted[i], "Concurrency should be resolved, every call should increment by one.");
            }
        }

        #endregion

        // TODO: CreatePutGetRangeDelete is way too complex as a unit test
        #region Methods

        /// <summary>
        /// Creates the new BLOB.
        /// </summary>
        /// <returns>
        /// The create new blob.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private string CreateNewBlob()
        {
            var name = "x" + Guid.NewGuid().ToString("N");
            this.BlobStorage.PutBlob(ContainerName, name, name);
            return name;
        }

        #endregion
    }
}