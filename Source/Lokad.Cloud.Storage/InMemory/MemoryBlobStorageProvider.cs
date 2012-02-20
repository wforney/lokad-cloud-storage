#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Mock in-memory Blob Storage.
    /// </summary>
    /// <remarks>
    /// All the methods of <see cref="MemoryBlobStorageProvider"/> are thread-safe. Note that the blob lease implementation is simplified such that leases do not time out.
    /// </remarks>
    public class MemoryBlobStorageProvider : IBlobStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        /// The containers.
        /// </summary>
        private readonly Dictionary<string, MockContainer> containers;

        /// <summary>
        ///   naive global lock to make methods thread-safe.
        /// </summary>
        private readonly object syncRoot;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryBlobStorageProvider"/> class. 
        ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryBlobStorageProvider()
        {
            this.containers = new Dictionary<string, MockContainer>();
            this.syncRoot = new object();
            this.DefaultSerializer = new CloudFormatter();
        }

        #endregion

        #region Properties

        /// <summary>
        ///   Gets or sets the default serializer.
        /// </summary>
        /// <value> The default serializer. </value>
        /// <remarks>
        /// </remarks>
        internal IDataSerializer DefaultSerializer { get; set; }

        /// <summary>
        /// Gets the containers.
        /// </summary>
        /// <remarks></remarks>
        private Dictionary<string, MockContainer> Containers
        {
            get
            {
                return this.containers;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Creates a new blob container.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the container was actually created and <c>false</c> if the container already exists. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool CreateContainerIfNotExist(string containerName)
        {
            lock (this.syncRoot)
            {
                if (!BlobStorageExtensions.IsContainerNameValid(containerName))
                {
                    throw new NotSupportedException(
                        "the containerName is not compliant with azure constraints on container names");
                }

                if (this.Containers.Keys.Contains(containerName))
                {
                    return false;
                }

                this.Containers.Add(containerName, new MockContainer());
                return true;
            }
        }

        /// <summary>
        /// Delete all blobs matching the provided blob name prefix.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobNamePrefix">
        /// The BLOB name prefix. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void DeleteAllBlobs(string containerName, string blobNamePrefix = null)
        {
            foreach (var blobName in this.ListBlobNames(containerName, blobNamePrefix))
            {
                this.DeleteBlobIfExist(containerName, blobName);
            }
        }

        /// <summary>
        /// Deletes a blob if it exists.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <returns>
        /// The delete blob if exist.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool DeleteBlobIfExist(string containerName, string blobName)
        {
            lock (this.syncRoot)
            {
                if (!this.Containers.Keys.Contains(containerName)
                    || !this.Containers[containerName].BlobNames.Contains(blobName))
                {
                    return false;
                }

                this.Containers[containerName].RemoveBlob(blobName);
                return true;
            }
        }

        /// <summary>
        /// Delete a container.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the container has actually been deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool DeleteContainerIfExist(string containerName)
        {
            lock (this.syncRoot)
            {
                if (!this.Containers.Keys.Contains(containerName))
                {
                    return false;
                }

                this.Containers.Remove(containerName);
                return true;
            }
        }

        /// <summary>
        /// Gets a blob.
        /// </summary>
        /// <typeparam name="T">
        /// The type of blob.
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// If there is no such blob, the returned object has its property HasValue set to <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> GetBlob<T>(string containerName, string blobName, IDataSerializer serializer = null)
        {
            string ignoredEtag;
            return this.GetBlob<T>(containerName, blobName, out ignoredEtag, serializer);
        }

        /// <summary>
        /// Gets a blob.
        /// </summary>
        /// <typeparam name="T">
        /// Blob type. 
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="etag">
        /// Identifier assigned by the storage to the blob that can be used to distinguish be successive version of the blob (useful to check for blob update). 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// If there is no such blob, the returned object has its property HasValue set to <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> GetBlob<T>(
            string containerName, string blobName, out string etag, IDataSerializer serializer = null)
        {
            return
                this.GetBlob(containerName, blobName, typeof(T), out etag, serializer).Convert(
                    o => o is T ? (T)o : Maybe<T>.Empty, Maybe<T>.Empty);
        }

        /// <summary>
        /// Gets a blob.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="type">
        /// The type of the blob. 
        /// </param>
        /// <param name="etag">
        /// Identifier assigned by the storage to the blob that can be used to distinguish be successive version of the blob (useful to check for blob update). 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// If there is no such blob, the returned object has its property HasValue set to <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<object> GetBlob(
            string containerName, string blobName, Type type, out string etag, IDataSerializer serializer = null)
        {
            lock (this.syncRoot)
            {
                if (!this.Containers.ContainsKey(containerName)
                    || !this.Containers[containerName].BlobNames.Contains(blobName))
                {
                    etag = null;
                    return Maybe<object>.Empty;
                }

                etag = this.Containers[containerName].BlobsEtag[blobName];
                return this.Containers[containerName].GetBlob(blobName);
            }
        }

        /// <summary>
        /// Gets the current etag of the blob, or <c>null</c> if the blob does not exists.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <returns>
        /// The get blob etag.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public string GetBlobEtag(string containerName, string blobName)
        {
            lock (this.syncRoot)
            {
                return (this.Containers.ContainsKey(containerName)
                        && this.Containers[containerName].BlobNames.Contains(blobName))
                           ? this.Containers[containerName].BlobsEtag[blobName]
                           : null;
            }
        }

        /// <summary>
        /// Gets a blob only if the etag has changed meantime.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the blob. 
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="oldEtag">
        /// Old etag value. If this value is <c>null</c> , the blob will always be retrieved (except if the blob does not exist anymore). 
        /// </param>
        /// <param name="newEtag">
        /// New etag value. Will be <c>null</c> if the blob no more exist, otherwise will be set to the current etag value of the blob. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// If the blob has not been modified or if there is no such blob, then the returned object has its property HasValue set to <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> GetBlobIfModified<T>(
            string containerName, string blobName, string oldEtag, out string newEtag, IDataSerializer serializer = null)
        {
            lock (this.syncRoot)
            {
                var currentEtag = this.GetBlobEtag(containerName, blobName);

                if (currentEtag == oldEtag)
                {
                    newEtag = null;
                    return Maybe<T>.Empty;
                }

                newEtag = currentEtag;
                return this.GetBlob<T>(containerName, blobName, serializer);
            }
        }

        /// <summary>
        /// Gets a range of blobs.
        /// </summary>
        /// <typeparam name="T">
        /// Blob type. 
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobNames">
        /// Names of the blobs. 
        /// </param>
        /// <param name="etags">
        /// Etag identifiers for all returned blobs. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// For each requested blob, an element in the array is returned in the same order. If a specific blob was not found, the corresponding <b>etags</b> array element is <c>null</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T>[] GetBlobRange<T>(
            string containerName, string[] blobNames, out string[] etags, IDataSerializer serializer = null)
        {
            var tempResult = blobNames.Select(
                blobName =>
                    {
                        string etag;
                        var blob = this.GetBlob<T>(containerName, blobName, out etag);
                        return new Tuple<Maybe<T>, string>(blob, etag);
                    }).ToArray();

            etags = new string[blobNames.Length];
            var result = new Maybe<T>[blobNames.Length];

            for (var i = 0; i < tempResult.Length; i++)
            {
                result[i] = tempResult[i].Item1;
                etags[i] = tempResult[i].Item2;
            }

            return result;
        }

        /// <summary>
        /// Gets a blob in intermediate XML representation for inspection and recovery, if supported by the serialization formatter.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="etag">
        /// Identifier assigned by the storage to the blob that can be used to distinguish be successive version of the blob (useful to check for blob update). 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// If there is no such blob or the formatter supports no XML representation, the returned object has its property HasValue set to <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<XElement> GetBlobXml(
            string containerName, string blobName, out string etag, IDataSerializer serializer = null)
        {
            etag = null;

            var formatter = (serializer ?? this.DefaultSerializer) as IIntermediateDataSerializer;
            if (formatter == null)
            {
                return Maybe<XElement>.Empty;
            }

            object data;
            lock (this.syncRoot)
            {
                if (!this.Containers.ContainsKey(containerName)
                    || !this.Containers[containerName].BlobNames.Contains(blobName))
                {
                    return Maybe<XElement>.Empty;
                }

                etag = this.Containers[containerName].BlobsEtag[blobName];
                data = this.Containers[containerName].GetBlob(blobName);
            }

            using (var stream = new MemoryStream())
            {
                formatter.Serialize(data, stream, data.GetType());
                stream.Position = 0;
                return formatter.UnpackXml(stream);
            }
        }

        /// <summary>
        /// Query whether a blob is locked by a blob lease.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <returns>
        /// <c>true</c> if [is BLOB locked] [the specified container name]; otherwise, <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool IsBlobLocked(string containerName, string blobName)
        {
            lock (this.syncRoot)
            {
                return (this.Containers.ContainsKey(containerName)
                        && this.Containers[containerName].BlobNames.Contains(blobName))
                       && this.Containers[containerName].BlobsLeases.ContainsKey(blobName);
            }
        }

        /// <summary>
        /// List the blob names of all blobs matching both the provided container name and the optional blob name prefix.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobNamePrefix">
        /// The BLOB name prefix. 
        /// </param>
        /// <returns>
        /// The blob names.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> ListBlobNames(string containerName, string blobNamePrefix = null)
        {
            lock (this.syncRoot)
            {
                if (!this.Containers.Keys.Contains(containerName))
                {
                    return Enumerable.Empty<string>();
                }

                var names = this.Containers[containerName].BlobNames;
                return string.IsNullOrEmpty(blobNamePrefix)
                           ? names
                           : names.Where(name => name.StartsWith(blobNamePrefix));
            }
        }

        /// <summary>
        /// List and get all blobs matching both the provided container name and the optional blob name prefix.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobNamePrefix">
        /// The BLOB name prefix. 
        /// </param>
        /// <param name="skip">
        /// The skip. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// An enumerable of T.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<T> ListBlobs<T>(
            string containerName, string blobNamePrefix = null, int skip = 0, IDataSerializer serializer = null)
        {
            var names = this.ListBlobNames(containerName, blobNamePrefix);

            if (skip > 0)
            {
                names = names.Skip(skip);
            }

            return
                names.Select(name => this.GetBlob<T>(containerName, name, serializer)).Where(blob => blob.HasValue).Select(blob => blob.Value);
        }

        /// <summary>
        /// Lists the containers.
        /// </summary>
        /// <param name="prefix">
        /// The prefix. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> ListContainers(string prefix = null)
        {
            lock (this.syncRoot)
            {
                if (string.IsNullOrEmpty(prefix))
                {
                    return this.Containers.Keys;
                }

                return this.Containers.Keys.Where(key => key.StartsWith(prefix));
            }
        }

        /// <summary>
        /// Puts a blob (overwrite if the blob already exists).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="item">
        /// The item. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void PutBlob<T>(string containerName, string blobName, T item, IDataSerializer serializer = null)
        {
            PutBlob(containerName, blobName, item, true, serializer);
        }

        /// <summary>
        /// Puts a blob and optionally overwrite.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="item">
        /// The item. 
        /// </param>
        /// <param name="overwrite">
        /// if set to <c>true</c> [overwrite]. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob has been put and <c>false</c> if the blob already exists but could not be overwritten. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool PutBlob<T>(
            string containerName, string blobName, T item, bool overwrite, IDataSerializer serializer = null)
        {
            string ignored;
            return this.PutBlob(containerName, blobName, item, overwrite, out ignored, serializer);
        }

        /// <summary>
        /// Puts a blob and optionally overwrite.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="item">
        /// Item to be put. 
        /// </param>
        /// <param name="overwrite">
        /// Indicates whether existing blob should be overwritten if it exists. 
        /// </param>
        /// <param name="etag">
        /// New etag (identifier used to track for blob change) if the blob is written, or <c>null</c> if no blob is written. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob has been put and <c>false</c> if the blob already exists but could not be overwritten. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool PutBlob<T>(
            string containerName, 
            string blobName, 
            T item, 
            bool overwrite, 
            out string etag, 
            IDataSerializer serializer = null)
        {
            return this.PutBlob(containerName, blobName, item, typeof(T), overwrite, out etag, serializer);
        }

        /// <summary>
        /// Puts a blob only if etag given in argument is matching blob's etag in blobStorage.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="item">
        /// Item to be put. 
        /// </param>
        /// <param name="expectedEtag">
        /// etag that should be matched inside BlobStorage. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob has been put and <c>false</c> if the blob already exists but version were not matching. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool PutBlob<T>(
            string containerName, string blobName, T item, string expectedEtag, IDataSerializer serializer = null)
        {
            string ignored;
            return this.PutBlob(containerName, blobName, item, typeof(T), true, expectedEtag, out ignored, serializer);
        }

        /// <summary>
        /// Puts a blob and optionally overwrite.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the blob. 
        /// </param>
        /// <param name="item">
        /// Item to be put. 
        /// </param>
        /// <param name="type">
        /// The type of the blob. 
        /// </param>
        /// <param name="overwrite">
        /// Indicates whether existing blob should be overwritten if it exists. 
        /// </param>
        /// <param name="etag">
        /// New etag (identifier used to track for blob change) if the blob is written, or <c>null</c> if no blob is written. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob has been put and <c>false</c> if the blob already exists but could not be overwritten. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool PutBlob(
            string containerName, 
            string blobName, 
            object item, 
            Type type, 
            bool overwrite, 
            out string etag, 
            IDataSerializer serializer = null)
        {
            return this.PutBlob(containerName, blobName, item, type, overwrite, null, out etag, serializer);
        }

        /// <summary>
        /// Puts the BLOB.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="item">
        /// The item. 
        /// </param>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <param name="overwrite">
        /// if set to <c>true</c> [overwrite]. 
        /// </param>
        /// <param name="expectedEtag">
        /// The expected etag. 
        /// </param>
        /// <param name="etag">
        /// The etag. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The put blob.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool PutBlob(
            string containerName, 
            string blobName, 
            object item, 
            Type type, 
            bool overwrite, 
            string expectedEtag, 
            out string etag, 
            IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? this.DefaultSerializer;
            lock (this.syncRoot)
            {
                etag = null;
                if (this.Containers.ContainsKey(containerName))
                {
                    if (this.Containers[containerName].BlobNames.Contains(blobName))
                    {
                        if (!overwrite
                            ||
                            expectedEtag != null && expectedEtag != this.Containers[containerName].BlobsEtag[blobName])
                        {
                            return false;
                        }

                        // Just verify that we can serialize
                        using (var stream = new MemoryStream())
                        {
                            dataSerializer.Serialize(item, stream, type);
                        }

                        this.Containers[containerName].SetBlob(blobName, item);
                        etag = this.Containers[containerName].BlobsEtag[blobName];
                        return true;
                    }

                    this.Containers[containerName].AddBlob(blobName, item);
                    etag = this.Containers[containerName].BlobsEtag[blobName];
                    return true;
                }

                if (!BlobStorageExtensions.IsContainerNameValid(containerName))
                {
                    throw new NotSupportedException(
                        "the containerName is not compliant with azure constraints on container names");
                }

                this.Containers.Add(containerName, new MockContainer());

                using (var stream = new MemoryStream())
                {
                    dataSerializer.Serialize(item, stream, type);
                }

                this.Containers[containerName].AddBlob(blobName, item);
                etag = this.Containers[containerName].BlobsEtag[blobName];
                return true;
            }
        }

        /// <summary>
        /// Requests a new lease on the blob and returns its new lease ID
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Result<string> TryAcquireLease(string containerName, string blobName)
        {
            lock (this.syncRoot)
            {
                if (!this.Containers[containerName].BlobsLeases.ContainsKey(blobName))
                {
                    var leaseId = Guid.NewGuid().ToString("N");
                    this.Containers[containerName].BlobsLeases[blobName] = leaseId;
                    return Result.CreateSuccess(leaseId);
                }

                return Result<string>.CreateError("Conflict");
            }
        }

        /// <summary>
        /// Releases the lease of the blob if the provided lease ID matches.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="leaseId">
        /// The lease id. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Result<string> TryReleaseLease(string containerName, string blobName, string leaseId)
        {
            lock (this.syncRoot)
            {
                string actualLeaseId;
                if (!this.Containers[containerName].BlobsLeases.TryGetValue(blobName, out actualLeaseId))
                {
                    return Result<string>.CreateError("NotFound");
                }

                if (actualLeaseId != leaseId)
                {
                    return Result<string>.CreateError("Conflict");
                }

                this.Containers[containerName].BlobsLeases.Remove(blobName);
                return Result.CreateSuccess("OK");
            }
        }

        /// <summary>
        /// Renews the lease of the blob if the provided lease ID matches.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="leaseId">
        /// The lease id. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Result<string> TryRenewLease(string containerName, string blobName, string leaseId)
        {
            lock (this.syncRoot)
            {
                string actualLeaseId;
                if (!this.Containers[containerName].BlobsLeases.TryGetValue(blobName, out actualLeaseId))
                {
                    return Result<string>.CreateError("NotFound");
                }

                if (actualLeaseId != leaseId)
                {
                    return Result<string>.CreateError("Conflict");
                }

                return Result.CreateSuccess("OK");
            }
        }

        /// <summary>
        /// Updates a blob if it already exists.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda, or empty if the blob did not exist. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> UpdateBlobIfExist<T>(
            string containerName, string blobName, Func<T, T> update, IDataSerializer serializer = null)
        {
            return this.UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, t => update(t), serializer);
        }

        /// <summary>
        /// Updates a blob if it already exists. If the insert or update lambdas return empty, the blob will be deleted.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda, or empty if the blob did not exist or was deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> UpdateBlobIfExistOrDelete<T>(
            string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            var result = this.UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, update, serializer);
            if (!result.HasValue)
            {
                this.DeleteBlobIfExist(containerName, blobName);
            }

            return result;
        }

        /// <summary>
        /// Updates a blob if it already exists. If the insert or update lambdas return empty, the blob will not be changed.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda, or empty if the blob did not exist or no change was applied. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> UpdateBlobIfExistOrSkip<T>(
            string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            return this.UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, update, serializer);
        }

        /// <summary>
        /// Inserts or updates a blob depending on whether it already exists or not.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="insert">
        /// The insert. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public T UpsertBlob<T>(
            string containerName, string blobName, Func<T> insert, Func<T, T> update, IDataSerializer serializer = null)
        {
            return this.UpsertBlobOrSkip<T>(containerName, blobName, () => insert(), t => update(t), serializer).Value;
        }

        /// <summary>
        /// Inserts or updates a blob depending on whether it already exists or not. If the insert or update lambdas return empty, the blob will be deleted (if it exists).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="insert">
        /// The insert. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda. If empty, then the blob has been deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> UpsertBlobOrDelete<T>(
            string containerName, 
            string blobName, 
            Func<Maybe<T>> insert, 
            Func<T, Maybe<T>> update, 
            IDataSerializer serializer = null)
        {
            var result = this.UpsertBlobOrSkip(containerName, blobName, insert, update, serializer);
            if (!result.HasValue)
            {
                this.DeleteBlobIfExist(containerName, blobName);
            }

            return result;
        }

        /// <summary>
        /// Inserts or updates a blob depending on whether it already exists or not. If the insert or update lambdas return empty, the blob will not be changed.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="insert">
        /// The insert. 
        /// </param>
        /// <param name="update">
        /// The update. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// The value returned by the lambda. If empty, then no change was applied. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> UpsertBlobOrSkip<T>(
            string containerName, 
            string blobName, 
            Func<Maybe<T>> insert, 
            Func<T, Maybe<T>> update, 
            IDataSerializer serializer = null)
        {
            lock (this.syncRoot)
            {
                Maybe<T> input;
                if (this.Containers.ContainsKey(containerName))
                {
                    if (this.Containers[containerName].BlobNames.Contains(blobName))
                    {
                        var blobData = this.Containers[containerName].GetBlob(blobName);
                        input = blobData == null ? Maybe<T>.Empty : (T)blobData;
                    }
                    else
                    {
                        input = Maybe<T>.Empty;
                    }
                }
                else
                {
                    this.Containers.Add(containerName, new MockContainer());
                    input = Maybe<T>.Empty;
                }

                var output = input.HasValue ? update(input.Value) : insert();

                if (output.HasValue)
                {
                    this.Containers[containerName].SetBlob(blobName, output.Value);
                }

                return output;
            }
        }

        #endregion

        /// <summary>
        /// The mock container.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class MockContainer
        {
            #region Constants and Fields

            /// <summary>
            /// The blob set.
            /// </summary>
            private readonly Dictionary<string, object> blobSet;

            /// <summary>
            /// The blobs etag.
            /// </summary>
            private readonly Dictionary<string, string> blobsEtag;

            /// <summary>
            /// The blobs leases.
            /// </summary>
            private readonly Dictionary<string, string> blobsLeases;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="MockContainer"/> class. 
            ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public MockContainer()
            {
                this.blobSet = new Dictionary<string, object>();
                this.blobsEtag = new Dictionary<string, string>();
                this.blobsLeases = new Dictionary<string, string>();
            }

            #endregion

            #region Public Properties

            /// <summary>
            ///   Gets the BLOB names.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public IEnumerable<string> BlobNames
            {
                get
                {
                    return this.blobSet.Keys.ToArray();
                }
            }

            /// <summary>
            ///   Gets the blobs etag.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public Dictionary<string, string> BlobsEtag
            {
                get
                {
                    return this.blobsEtag;
                }
            }

            /// <summary>
            ///   Gets the blobs leases.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public Dictionary<string, string> BlobsLeases
            {
                get
                {
                    return this.blobsLeases;
                }
            }

            #endregion

            #region Public Methods and Operators

            /// <summary>
            /// Adds the BLOB.
            /// </summary>
            /// <param name="blobName">
            /// Name of the BLOB. 
            /// </param>
            /// <param name="item">
            /// The item. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public void AddBlob(string blobName, object item)
            {
                this.blobSet.Add(blobName, item);
                this.blobsEtag.Add(blobName, Guid.NewGuid().ToString());
            }

            /// <summary>
            /// Gets the BLOB.
            /// </summary>
            /// <param name="blobName">
            /// Name of the BLOB. 
            /// </param>
            /// <returns>
            /// The get blob.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public object GetBlob(string blobName)
            {
                return this.blobSet[blobName];
            }

            /// <summary>
            /// Removes the BLOB.
            /// </summary>
            /// <param name="blobName">
            /// Name of the BLOB. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public void RemoveBlob(string blobName)
            {
                this.blobSet.Remove(blobName);
                this.blobsEtag.Remove(blobName);
                this.blobsLeases.Remove(blobName);
            }

            /// <summary>
            /// Sets the BLOB.
            /// </summary>
            /// <param name="blobName">
            /// Name of the BLOB. 
            /// </param>
            /// <param name="item">
            /// The item. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public void SetBlob(string blobName, object item)
            {
                this.blobSet[blobName] = item;
                this.blobsEtag[blobName] = Guid.NewGuid().ToString();
            }

            #endregion
        }
    }
}