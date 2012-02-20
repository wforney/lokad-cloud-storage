#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;
    using System.Collections.Generic;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Abstraction for the Blob Storage.
    /// </summary>
    /// <remarks>
    /// This provider represents a <em>logical</em> blob storage, not the actual Blob Storage. In particular, this provider deals with overflowing buffers that need to be split in smaller chunks to be uploaded.
    /// </remarks>
    public interface IBlobStorageProvider
    {
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
        /// This operation is idempotent.
        /// </remarks>
        bool CreateContainerIfNotExist(string containerName);

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
        /// This method is idempotent.
        /// </remarks>
        void DeleteAllBlobs(string containerName, string blobNamePrefix = null);

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
        /// This method is idempotent.
        /// </remarks>
        bool DeleteBlobIfExist(string containerName, string blobName);

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
        /// This operation is idempotent.
        /// </remarks>
        bool DeleteContainerIfExist(string containerName);

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
        Maybe<T> GetBlob<T>(string containerName, string blobName, IDataSerializer serializer = null);

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
        Maybe<T> GetBlob<T>(string containerName, string blobName, out string etag, IDataSerializer serializer = null);

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
        /// This method should only be used when the caller does not know the type of the object stored in the blob at compile time, but it can only be determined at run time. In all other cases, you should use the generic overloads of the method.
        /// </remarks>
        Maybe<object> GetBlob(
            string containerName, string blobName, Type type, out string etag, IDataSerializer serializer = null);

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
        string GetBlobEtag(string containerName, string blobName);

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
        Maybe<T> GetBlobIfModified<T>(
            string containerName, string blobName, string oldEtag, out string newEtag, IDataSerializer serializer = null);

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
        Maybe<T>[] GetBlobRange<T>(
            string containerName, string[] blobNames, out string[] etags, IDataSerializer serializer = null);

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
        Maybe<XElement> GetBlobXml(
            string containerName, string blobName, out string etag, IDataSerializer serializer = null);

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
        bool IsBlobLocked(string containerName, string blobName);

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
        /// This method is sideeffect-free, except for infrastructure effects like thread pool usage.
        /// </remarks>
        IEnumerable<string> ListBlobNames(string containerName, string blobNamePrefix = null);

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
        /// This method is sideeffect-free, except for infrastructure effects like thread pool usage.
        /// </remarks>
        IEnumerable<T> ListBlobs<T>(
            string containerName, string blobNamePrefix = null, int skip = 0, IDataSerializer serializer = null);

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
        IEnumerable<string> ListContainers(string containerNamePrefix = null);

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
        /// Creates the container if it does not exist beforehand.
        /// </remarks>
        void PutBlob<T>(string containerName, string blobName, T item, IDataSerializer serializer = null);

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
        /// Creates the container if it does not exist beforehand.
        /// </remarks>
        bool PutBlob<T>(
            string containerName, string blobName, T item, bool overwrite, IDataSerializer serializer = null);

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
        /// Creates the container if it does not exist beforehand.
        /// </remarks>
        bool PutBlob<T>(
            string containerName, 
            string blobName, 
            T item, 
            bool overwrite, 
            out string etag, 
            IDataSerializer serializer = null);

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
        /// Creates the container if it does not exist beforehand.
        /// </remarks>
        bool PutBlob<T>(
            string containerName, string blobName, T item, string expectedEtag, IDataSerializer serializer = null);

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
        /// Creates the container if it does not exist beforehand.
        /// </remarks>
        bool PutBlob(
            string containerName, 
            string blobName, 
            object item, 
            Type type, 
            bool overwrite, 
            out string etag, 
            IDataSerializer serializer = null);

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
        Result<string> TryAcquireLease(string containerName, string blobName);

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
        Result<string> TryReleaseLease(string containerName, string blobName, string leaseId);

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
        Result<string> TryRenewLease(string containerName, string blobName, string leaseId);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent.
        /// </para>
        /// </remarks>
        Maybe<T> UpdateBlobIfExist<T>(
            string containerName, string blobName, Func<T, T> update, IDataSerializer serializer = null);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent.
        /// </para>
        /// </remarks>
        Maybe<T> UpdateBlobIfExistOrDelete<T>(
            string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent.
        /// </para>
        /// </remarks>
        Maybe<T> UpdateBlobIfExistOrSkip<T>(
            string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent
        ///                                                     and if the object returned by the insert lambda is an invariant to the update lambda
        ///                                                     (if the second condition is not met, it is idempotent after the first successful call).
        /// </para>
        /// </remarks>
        T UpsertBlob<T>(
            string containerName, string blobName, Func<T> insert, Func<T, T> update, IDataSerializer serializer = null);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent
        ///                                                     and if the object returned by the insert lambda is an invariant to the update lambda
        ///                                                     (if the second condition is not met, it is idempotent after the first successful call).
        /// </para>
        /// </remarks>
        Maybe<T> UpsertBlobOrDelete<T>(
            string containerName, 
            string blobName, 
            Func<Maybe<T>> insert, 
            Func<T, Maybe<T>> update, 
            IDataSerializer serializer = null);

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
        /// <para>
        /// The provided lambdas can be executed multiple times in case of
        ///     concurrency-related retrials, so be careful with side-effects
        ///     (like incrementing a counter in them).
        /// </para>
        /// <para>
        /// This method is idempotent if and only if the provided lambdas are idempotent
        ///                                                     and if the object returned by the insert lambda is an invariant to the update lambda
        ///                                                     (if the second condition is not met, it is idempotent after the first successful call).
        /// </para>
        /// </remarks>
        Maybe<T> UpsertBlobOrSkip<T>(
            string containerName, 
            string blobName, 
            Func<Maybe<T>> insert, 
            Func<T, Maybe<T>> update, 
            IDataSerializer serializer = null);

        #endregion
    }
}