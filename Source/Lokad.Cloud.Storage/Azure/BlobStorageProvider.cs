#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Instrumentation;
    using Lokad.Cloud.Storage.Instrumentation.Events;
    using Lokad.Cloud.Storage.Shared.Monads;
    using Lokad.Cloud.Storage.Shared.Threading;

    using Microsoft.WindowsAzure.StorageClient;
    using Microsoft.WindowsAzure.StorageClient.Protocol;

    /// <summary>
    /// Provides access to the Blob Storage.
    /// </summary>
    /// <remarks>
    /// All the methods of <see cref="BlobStorageProvider"/> are thread-safe.
    /// </remarks>
    public class BlobStorageProvider : IBlobStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        ///   Custom meta-data used as a work-around of an issue of the StorageClient.
        /// </summary>
        private const string MetadataMD5Key = "LokadContentMD5";

        /// <summary>
        /// The blob storage.
        /// </summary>
        private readonly CloudBlobClient blobStorage;

        /// <summary>
        /// The default serializer.
        /// </summary>
        private readonly IDataSerializer defaultSerializer;

        /// <summary>
        /// The observer.
        /// </summary>
        private readonly IStorageObserver observer;

        /// <summary>
        /// The policies.
        /// </summary>
        private readonly RetryPolicies policies;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobStorageProvider"/> class. 
        /// IoC constructor.
        /// </summary>
        /// <param name="blobStorage">
        /// The BLOB storage. 
        /// </param>
        /// <param name="defaultSerializer">
        /// The default serializer. 
        /// </param>
        /// <param name="observer">
        /// The observer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public BlobStorageProvider(
            CloudBlobClient blobStorage, IDataSerializer defaultSerializer, IStorageObserver observer = null)
        {
            this.policies = new RetryPolicies(observer);
            this.blobStorage = blobStorage;
            this.defaultSerializer = defaultSerializer;
            this.observer = observer;
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
            // workaround since Azure is presently returning OutOfRange exception when using a wrong name.
            if (!BlobStorageExtensions.IsContainerNameValid(containerName))
            {
                throw new NotSupportedException(
                    "containerName is not compliant with azure constraints on container naming");
            }

            var container = this.blobStorage.GetContainerReference(containerName);
            try
            {
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, container.Create);
                return true;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerAlreadyExists
                    || ex.ErrorCode == StorageErrorCode.ResourceAlreadyExists)
                {
                    return false;
                }

                throw;
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
            Parallel.ForEach(
                this.ListBlobNames(containerName, blobNamePrefix),
                blobName => this.DeleteBlobIfExist(containerName, blobName));
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
            var stopwatch = Stopwatch.StartNew();

            var container = this.blobStorage.GetContainerReference(containerName);

            try
            {
                var blob = container.GetBlockBlobReference(blobName);
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, blob.Delete);

                this.NotifySucceeded(StorageOperationType.BlobDelete, stopwatch);
                return true;
            }
            catch (StorageClientException ex)
            {
                // no such container, return false
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    // success anyway since the condition was not met
                    this.NotifySucceeded(StorageOperationType.BlobDelete, stopwatch);
                    return false;
                }

                throw;
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
            var container = this.blobStorage.GetContainerReference(containerName);
            try
            {
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, container.Delete);
                return true;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }

                throw;
            }
        }

        /// <summary>
        /// Gets a blob.
        /// </summary>
        /// <typeparam name="T">
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
            return this.GetBlob(containerName, blobName, typeof(T), out etag, serializer).Convert(
                o => (T)o, Maybe<T>.Empty);
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
            var stopwatch = Stopwatch.StartNew();

            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            var stream = new MemoryStream();
            etag = null;

            // if no such container, return empty
            try
            {
                Retry.Do(
                    this.policies.NetworkCorruption,
                    this.policies.TransientServerErrorBackOff,
                    CancellationToken.None,
                    () =>
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        blob.DownloadToStream(stream);
                        VerifyContentHash(blob, stream, containerName, blobName);
                    });

                etag = blob.Properties.ETag;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return Maybe<object>.Empty;
                }

                throw;
            }

            stream.Seek(0, SeekOrigin.Begin);
            var deserialized = (serializer ?? this.defaultSerializer).TryDeserialize(stream, type);

            if (this.observer != null)
            {
                if (!deserialized.IsSuccess)
                {
                    this.observer.Notify(
                        new BlobDeserializationFailedEvent(deserialized.Error, containerName, blobName));
                }
                else
                {
                    this.NotifySucceeded(StorageOperationType.BlobGet, stopwatch);
                }
            }

            return deserialized.IsSuccess ? new Maybe<object>(deserialized.Value) : Maybe<object>.Empty;
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
            var container = this.blobStorage.GetContainerReference(containerName);

            try
            {
                var blob = container.GetBlockBlobReference(blobName);
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, blob.FetchAttributes);
                return blob.Properties.ETag;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return null;
                }

                throw;
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
            var dataSerializer = serializer ?? this.defaultSerializer;

            // 'oldEtag' is null, then behavior always match simple 'GetBlob'.
            if (null == oldEtag)
            {
                return this.GetBlob<T>(containerName, blobName, out newEtag, dataSerializer);
            }

            var stopwatch = Stopwatch.StartNew();

            newEtag = null;

            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            try
            {
                var options = new BlobRequestOptions { AccessCondition = AccessCondition.IfNoneMatch(oldEtag) };

                var stream = new MemoryStream();
                Retry.Do(
                    this.policies.NetworkCorruption,
                    this.policies.TransientServerErrorBackOff,
                    CancellationToken.None,
                    () =>
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        blob.DownloadToStream(stream, options);
                        VerifyContentHash(blob, stream, containerName, blobName);
                    });

                newEtag = blob.Properties.ETag;

                stream.Seek(0, SeekOrigin.Begin);
                var deserialized = dataSerializer.TryDeserializeAs<T>(stream);

                if (this.observer != null)
                {
                    if (!deserialized.IsSuccess)
                    {
                        this.observer.Notify(
                            new BlobDeserializationFailedEvent(deserialized.Error, containerName, blobName));
                    }
                    else
                    {
                        this.NotifySucceeded(StorageOperationType.BlobGetIfModified, stopwatch);
                    }
                }

                return deserialized.IsSuccess ? deserialized.Value : Maybe<T>.Empty;
            }
            catch (StorageClientException ex)
            {
                // call fails because blob has not been modified (usual case)
                if (ex.ErrorCode == StorageErrorCode.ConditionFailed || // HACK: BUG in StorageClient 1.0 
                    // see http://social.msdn.microsoft.com/Forums/en-US/windowsazure/thread/4817cafa-12d8-4979-b6a7-7bda053e6b21
                    ex.Message == @"The condition specified using HTTP conditional header(s) is not met.")
                {
                    return Maybe<T>.Empty;
                }

                // call fails due to misc problems
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return Maybe<T>.Empty;
                }

                throw;
            }
        }

        /// <summary>
        /// As many parallel requests than there are blob names.
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
            var dataSerializer = serializer ?? this.defaultSerializer;
            var tempResult = blobNames.SelectInParallel(
                blobName =>
                {
                    string etag;
                    var blob = this.GetBlob<T>(containerName, blobName, out etag, dataSerializer);
                    return new Tuple<Maybe<T>, string>(blob, etag);
                },
                blobNames.Length);

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

            var formatter = (serializer ?? this.defaultSerializer) as IIntermediateDataSerializer;
            if (formatter == null)
            {
                return Maybe<XElement>.Empty;
            }

            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            var stream = new MemoryStream();

            // if no such container, return empty
            try
            {
                Retry.Do(
                    this.policies.NetworkCorruption,
                    this.policies.TransientServerErrorBackOff,
                    CancellationToken.None,
                    () =>
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        blob.DownloadToStream(stream);
                        VerifyContentHash(blob, stream, containerName, blobName);
                    });

                etag = blob.Properties.ETag;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return Maybe<XElement>.Empty;
                }

                throw;
            }

            stream.Seek(0, SeekOrigin.Begin);
            var unpacked = formatter.TryUnpackXml(stream);
            return unpacked.IsSuccess ? unpacked.Value : Maybe<XElement>.Empty;
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
            var container = this.blobStorage.GetContainerReference(containerName);

            try
            {
                var blob = container.GetBlockBlobReference(blobName);
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, blob.FetchAttributes);
                return blob.Properties.LeaseStatus == LeaseStatus.Locked;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound || ex.ErrorCode == StorageErrorCode.BlobNotFound
                    || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }

                throw;
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
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> ListBlobNames(string containerName, string blobNamePrefix = null)
        {
            // Enumerated blobs do not have a "name" property,
            // thus the name must be extracted from their URI
            // http://social.msdn.microsoft.com/Forums/en-US/windowsazure/thread/c5e36676-8d07-46cc-b803-72621a0898b0/?prof=required
            if (blobNamePrefix == null)
            {
                blobNamePrefix = string.Empty;
            }

            var container = this.blobStorage.GetContainerReference(containerName);

            var options = new BlobRequestOptions { UseFlatBlobListing = true };

            // if no prefix is provided, then enumerate the whole container
            IEnumerator<IListBlobItem> enumerator;
            if (string.IsNullOrEmpty(blobNamePrefix))
            {
                enumerator = container.ListBlobs(options).GetEnumerator();
            }
            else
            {
                // 'CloudBlobDirectory' must be used for prefixed enumeration
                var directory = container.GetDirectoryReference(blobNamePrefix);

                // HACK: [vermorel] very ugly override, but otherwise an "/" separator gets forcibly added
                var fieldInfo = typeof(CloudBlobDirectory).GetField("prefix", BindingFlags.Instance | BindingFlags.NonPublic);
                if (fieldInfo != null)
                {
                    fieldInfo.SetValue(directory, blobNamePrefix);
                }

                enumerator = directory.ListBlobs(options).GetEnumerator();
            }

            // TODO: Parallelize
            while (true)
            {
                try
                {
                    if (
                        !Retry.Get(
                            this.policies.TransientServerErrorBackOff, CancellationToken.None, enumerator.MoveNext))
                    {
                        yield break;
                    }
                }
                catch (StorageClientException ex)
                {
                    // if the container does not exist, empty enumeration
                    if (ex.ErrorCode == StorageErrorCode.ContainerNotFound)
                    {
                        yield break;
                    }

                    throw;
                }

                // removing /container/ from the blob name (dev storage: /account/container/)
                yield return
                    Uri.UnescapeDataString(
                        enumerator.Current.Uri.AbsolutePath.Substring(container.Uri.LocalPath.Length + 1));
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
        /// List the names of all containers, matching the optional prefix if provided.
        /// </summary>
        /// <param name="containerNamePrefix">
        /// The container name prefix. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> ListContainers(string containerNamePrefix = null)
        {
            var enumerator = string.IsNullOrEmpty(containerNamePrefix)
                                 ? this.blobStorage.ListContainers().GetEnumerator()
                                 : this.blobStorage.ListContainers(containerNamePrefix).GetEnumerator();

            // TODO: Parallelize
            while (true)
            {
                if (!Retry.Get(this.policies.TransientServerErrorBackOff, CancellationToken.None, enumerator.MoveNext))
                {
                    yield break;
                }

                // removing /container/ from the blob name (dev storage: /account/container/)
                yield return enumerator.Current.Name;
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
            string outEtag;
            return this.PutBlob(containerName, blobName, item, typeof(T), true, expectedEtag, out outEtag, serializer);
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
        /// <param name="outEtag">
        /// The out etag. 
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
            out string outEtag,
            IDataSerializer serializer = null)
        {
            var stopwatch = Stopwatch.StartNew();

            var stream = new MemoryStream();
            (serializer ?? this.defaultSerializer).Serialize(item, stream, type);

            var container = this.blobStorage.GetContainerReference(containerName);

            Func<Maybe<string>> doUpload = () =>
                {
                    var blob = container.GetBlockBlobReference(blobName);

                    // single remote call
                    var result = this.UploadBlobContent(blob, stream, overwrite, expectedEtag);

                    return result;
                };

            try
            {
                var result = doUpload();
                if (!result.HasValue)
                {
                    outEtag = null;
                    return false;
                }

                outEtag = result.Value;
                this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                return true;
            }
            catch (StorageClientException ex)
            {
                // if the container does not exist, it gets created
                if (ex.ErrorCode == StorageErrorCode.ContainerNotFound)
                {
                    // caution: the container might have been freshly deleted
                    // (multiple retries are needed in such a situation)
                    var tentativeEtag = Maybe<string>.Empty;
                    Retry.Do(
                        this.policies.SlowInstantiation,
                        CancellationToken.None,
                        () =>
                        {
                            Retry.Get(
                                this.policies.TransientServerErrorBackOff,
                                CancellationToken.None,
                                container.CreateIfNotExist);

                            tentativeEtag = doUpload();
                        });

                    if (!tentativeEtag.HasValue)
                    {
                        outEtag = null;

                        // success because it behaved as excpected - the expected etag was not matching so it was not overwritten
                        this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                        return false;
                    }

                    outEtag = tentativeEtag.Value;
                    this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                    return true;
                }

                if (ex.ErrorCode == StorageErrorCode.BlobAlreadyExists && !overwrite)
                {
                    // See http://social.msdn.microsoft.com/Forums/en-US/windowsazure/thread/fff78a35-3242-4186-8aee-90d27fbfbfd4
                    // and http://social.msdn.microsoft.com/Forums/en-US/windowsazure/thread/86b9f184-c329-4c30-928f-2991f31e904b/
                    outEtag = null;

                    // success because it behaved as excpected - the expected etag was not matching so it was not overwritten
                    this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                    return false;
                }

                var result = doUpload();
                if (!result.HasValue)
                {
                    outEtag = null;

                    // success because it behaved as excpected - the expected etag was not matching so it was not overwritten
                    this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                    return false;
                }

                outEtag = result.Value;
                this.NotifySucceeded(StorageOperationType.BlobPut, stopwatch);
                return true;
            }
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
        /// <param name="outEtag">
        /// The out etag. 
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
            out string outEtag,
            IDataSerializer serializer = null)
        {
            return this.PutBlob(containerName, blobName, item, type, overwrite, null, out outEtag, serializer);
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
            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            var credentials = this.blobStorage.Credentials;
            var uri = new Uri(credentials.TransformUri(blob.Uri.ToString()));
            var request = BlobRequest.Lease(uri, 90, LeaseAction.Acquire, null);
            credentials.SignRequest(request);

            HttpWebResponse response;
            try
            {
                response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException we)
            {
                var statusCode = ((HttpWebResponse)we.Response).StatusCode;
                switch (statusCode)
                {
                    case HttpStatusCode.Conflict:
                    case HttpStatusCode.NotFound:
                    case HttpStatusCode.RequestTimeout:
                    case HttpStatusCode.InternalServerError:
                        return Result<string>.CreateError(statusCode.ToString());
                    default:
                        throw;
                }
            }

            try
            {
                return response.StatusCode == HttpStatusCode.Created
                           ? Result<string>.CreateSuccess(response.Headers["x-ms-lease-id"])
                           : Result<string>.CreateError(response.StatusCode.ToString());
            }
            finally
            {
                response.Close();
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
            return this.TryLeaseAction(containerName, blobName, LeaseAction.Release, leaseId);
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
            return this.TryLeaseAction(containerName, blobName, LeaseAction.Renew, leaseId);
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
        /// The type.
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
            var stopwatch = Stopwatch.StartNew();
            var dataSerializer = serializer ?? this.defaultSerializer;

            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            var optimisticPolicy = this.policies.OptimisticConcurrency();
            var retryInterval = TimeSpan.Zero;
            var retryCount = 0;
            do
            {
                // 0. IN CASE OF RETRIAL, WAIT UNTIL NEXT TRIAL (retry policy)
                if (retryInterval > TimeSpan.Zero)
                {
                    Thread.Sleep(retryInterval);
                }

                // 1. DOWNLOAD EXISTING INPUT BLOB, IF IT EXISTS
                Maybe<T> input;
                var inputBlobExists = false;
                string inputETag = null;

                try
                {
                    var readStream = new MemoryStream();
                    Retry.Do(
                        this.policies.NetworkCorruption,
                        this.policies.TransientServerErrorBackOff,
                        CancellationToken.None,
                        () =>
                        {
                            readStream.Seek(0, SeekOrigin.Begin);
                            blob.DownloadToStream(readStream);
                            VerifyContentHash(blob, readStream, containerName, blobName);
                        });

                    inputETag = blob.Properties.ETag;
                    inputBlobExists = !string.IsNullOrEmpty(inputETag);

                    readStream.Seek(0, SeekOrigin.Begin);

                    var deserialized = dataSerializer.TryDeserializeAs<T>(readStream);
                    if (!deserialized.IsSuccess && this.observer != null)
                    {
                        this.observer.Notify(
                            new BlobDeserializationFailedEvent(deserialized.Error, containerName, blobName));
                    }

                    input = deserialized.IsSuccess ? deserialized.Value : Maybe<T>.Empty;
                }
                catch (StorageClientException ex)
                {
                    // creating the container when missing
                    if (ex.ErrorCode == StorageErrorCode.ContainerNotFound
                        || ex.ErrorCode == StorageErrorCode.BlobNotFound
                        || ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                    {
                        input = Maybe<T>.Empty;

                        // caution: the container might have been freshly deleted
                        // (multiple retries are needed in such a situation)
                        Retry.Get(
                            this.policies.SlowInstantiation,
                            this.policies.TransientServerErrorBackOff,
                            CancellationToken.None,
                            container.CreateIfNotExist);
                    }
                    else
                    {
                        throw;
                    }
                }

                // 2. APPLY UPADTE OR INSERT (DEPENDING ON INPUT)
                var output = input.HasValue ? update(input.Value) : insert();

                // 3. IF EMPTY OUTPUT THEN WE CAN SKIP THE WHOLE OPERATION
                if (!output.HasValue)
                {
                    this.NotifySucceeded(StorageOperationType.BlobUpsertOrSkip, stopwatch);
                    return output;
                }

                // 4. TRY TO INSERT OR UPDATE BLOB
                using (var writeStream = new MemoryStream())
                {
                    dataSerializer.Serialize(output.Value, writeStream, typeof(T));
                    writeStream.Seek(0, SeekOrigin.Begin);

                    // Semantics:
                    // Insert: Blob must not exist -> do not overwrite
                    // Update: Blob must exists -> overwrite and verify matching ETag
                    var succeeded = inputBlobExists
                                        ? this.UploadBlobContent(blob, writeStream, true, inputETag).HasValue
                                        : this.UploadBlobContent(blob, writeStream, false, null).HasValue;
                    if (succeeded)
                    {
                        this.NotifySucceeded(StorageOperationType.BlobUpsertOrSkip, stopwatch);
                        return output;
                    }
                }
            }
            while (optimisticPolicy(retryCount++, null, out retryInterval));

            throw new TimeoutException(
                "Failed to resolve optimistic concurrency errors within a limited number of retrials");
        }

        #endregion

        #region Methods

        /// <summary>
        /// Apply a content hash to the blob to verify upload and roundtrip consistency.
        /// </summary>
        /// <param name="blob">
        /// The BLOB. 
        /// </param>
        /// <param name="stream">
        /// The stream. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private static void ApplyContentHash(CloudBlob blob, Stream stream)
        {
            var hash = ComputeContentHash(stream);

            // HACK: [Vermorel 2010-11] StorageClient does not apply MD5 on smaller blobs.
            // Reflector indicates that the behavior threshold is at 32MB
            // so manually disable hasing for larger blobs
            if (stream.Length < 0x2000000L)
            {
                blob.Properties.ContentMD5 = hash;
            }

            // HACK: [vermorel 2010-11] StorageClient does not provide a way to retrieve
            // MD5 so we add our own MD5 check which let perform our own validation when
            // downloading the blob (full roundtrip validation). 
            blob.Metadata[MetadataMD5Key] = hash;
        }

        /// <summary>
        /// Computes the content hash.
        /// </summary>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <returns>
        /// The compute content hash.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static string ComputeContentHash(Stream source)
        {
            byte[] hash;
            source.Seek(0, SeekOrigin.Begin);
            using (var md5 = MD5.Create())
            {
                hash = md5.ComputeHash(source);
            }

            source.Seek(0, SeekOrigin.Begin);
            return Convert.ToBase64String(hash);
        }

        /// <summary>
        /// Throws a DataCorruptionException if the content hash is available but doesn't match.
        /// </summary>
        /// <param name="blob">
        /// The BLOB. 
        /// </param>
        /// <param name="stream">
        /// The stream. 
        /// </param>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private static void VerifyContentHash(CloudBlob blob, Stream stream, string containerName, string blobName)
        {
            var expectedHash = blob.Metadata[MetadataMD5Key];
            if (string.IsNullOrEmpty(expectedHash))
            {
                return;
            }

            if (expectedHash != ComputeContentHash(stream))
            {
                throw new DataCorruptionException(
                    string.Format("MD5 mismatch on blob retrieval {0}/{1}.", containerName, blobName));
            }
        }

        /// <summary>
        /// Notifies the succeeded.
        /// </summary>
        /// <param name="operationType">
        /// Type of the operation. 
        /// </param>
        /// <param name="stopwatch">
        /// The stopwatch. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void NotifySucceeded(StorageOperationType operationType, Stopwatch stopwatch)
        {
            if (this.observer != null)
            {
                this.observer.Notify(new StorageOperationSucceededEvent(operationType, stopwatch.Elapsed));
            }
        }

        /// <summary>
        /// Tries the lease action.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <param name="leaseId">
        /// The lease id. 
        /// </param>
        /// <returns>
        /// The string.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private Result<string> TryLeaseAction(
            string containerName, string blobName, LeaseAction action, string leaseId = null)
        {
            var container = this.blobStorage.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            var credentials = this.blobStorage.Credentials;
            var uri = new Uri(credentials.TransformUri(blob.Uri.ToString()));
            var request = BlobRequest.Lease(uri, 90, action, leaseId);
            credentials.SignRequest(request);

            HttpWebResponse response;
            try
            {
                response = (HttpWebResponse)request.GetResponse();
            }
            catch (WebException we)
            {
                var statusCode = ((HttpWebResponse)we.Response).StatusCode;
                switch (statusCode)
                {
                    case HttpStatusCode.Conflict:
                    case HttpStatusCode.NotFound:
                    case HttpStatusCode.RequestTimeout:
                    case HttpStatusCode.InternalServerError:
                        return Result<string>.CreateError(statusCode.ToString());
                    default:
                        throw;
                }
            }

            try
            {
                var expectedCode = action == LeaseAction.Break ? HttpStatusCode.Accepted : HttpStatusCode.OK;
                return response.StatusCode == expectedCode
                           ? Result<string>.CreateSuccess("OK")
                           : Result<string>.CreateError(response.StatusCode.ToString());
            }
            finally
            {
                response.Close();
            }
        }

        /// <summary>
        /// Uploads the content of the BLOB.
        /// </summary>
        /// <param name="blob">
        /// The BLOB. 
        /// </param>
        /// <param name="stream">
        /// The stream. 
        /// </param>
        /// <param name="overwrite">
        /// If <c>false</c> , then no write happens if the blob already exists. 
        /// </param>
        /// <param name="expectedEtag">
        /// When specified, no writing occurs unless the blob etag matches the one specified as argument. 
        /// </param>
        /// <returns>
        /// The ETag of the written blob, if it was written. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        private Maybe<string> UploadBlobContent(CloudBlob blob, Stream stream, bool overwrite, string expectedEtag)
        {
            BlobRequestOptions options;

            if (!overwrite)
            {
                // no overwrite authorized, blob must NOT exists
                options = new BlobRequestOptions
                    {
                        AccessCondition = AccessCondition.IfNotModifiedSince(DateTime.MinValue)
                    };
            }
            else
            {
                // overwrite is OK
                options = string.IsNullOrEmpty(expectedEtag)
                              ? // case with no etag constraint
                          new BlobRequestOptions { AccessCondition = AccessCondition.None }
                              : // case with etag constraint
                          new BlobRequestOptions { AccessCondition = AccessCondition.IfMatch(expectedEtag) };
            }

            ApplyContentHash(blob, stream);

            try
            {
                Retry.Do(
                    this.policies.NetworkCorruption,
                    this.policies.TransientServerErrorBackOff,
                    CancellationToken.None,
                    () =>
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        blob.UploadFromStream(stream, options);
                    });
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ConditionFailed)
                {
                    return Maybe<string>.Empty;
                }

                throw;
            }

            return blob.Properties.ETag;
        }

        #endregion
    }
}