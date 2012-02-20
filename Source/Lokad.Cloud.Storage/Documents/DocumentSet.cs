#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Documents
{
    using System;
    using System.Collections.Generic;

    using Lokad.Cloud.Storage.Blobs;

    /// <summary>
    /// Represents a set of documents and how they are persisted.
    /// </summary>
    /// <typeparam name="TDocument">
    /// The type of the document. 
    /// </typeparam>
    /// <typeparam name="TKey">
    /// The type of the key. 
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    public class DocumentSet<TDocument, TKey> : IDocumentSet<TDocument, TKey>
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSet{TDocument,TKey}"/> class.
        /// </summary>
        /// <param name="blobs">
        /// The blobs. 
        /// </param>
        /// <param name="locationOfKey">
        /// The location of key. 
        /// </param>
        /// <param name="commonPrefix">
        /// The common prefix. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public DocumentSet(
            IBlobStorageProvider blobs, 
            Func<TKey, IBlobLocation> locationOfKey, 
            Func<IBlobLocation> commonPrefix = null, 
            IDataSerializer serializer = null)
        {
            this.Blobs = blobs;
            this.Serializer = serializer;
            this.LocationOfKey = locationOfKey;
            this.CommonPrefixLocation = commonPrefix;
        }

        #endregion

        #region Properties

        /// <summary>
        ///   Gets the blobs.
        /// </summary>
        /// <remarks>
        /// </remarks>
        protected IBlobStorageProvider Blobs { get; private set; }

        /// <summary>
        ///   Gets the common prefix location.
        /// </summary>
        /// <remarks>
        /// </remarks>
        protected Func<IBlobLocation> CommonPrefixLocation { get; private set; }

        /// <summary>
        ///   Gets the location of key.
        /// </summary>
        /// <remarks>
        /// </remarks>
        protected Func<TKey, IBlobLocation> LocationOfKey { get; private set; }

        /// <summary>
        ///   Gets or sets the serializer.
        /// </summary>
        /// <value> The serializer. </value>
        /// <remarks>
        /// </remarks>
        protected IDataSerializer Serializer { get; set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Delete all document matching the provided prefix. Not all document sets will support this, those that do not will throw a NotSupportedException.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// </exception>
        /// <remarks>
        /// </remarks>
        public void DeleteAll()
        {
            if (this.CommonPrefixLocation == null)
            {
                throw new NotSupportedException();
            }

            this.DeleteAllInternal(this.CommonPrefixLocation());
        }

        /// <summary>
        /// Delete the document, if it exists.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <returns>
        /// The delete if exist. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool DeleteIfExist(TKey key)
        {
            var location = this.LocationOfKey(key);
            this.RemoveCache(location);
            return this.Blobs.DeleteBlobIfExist(location);
        }

        /// <summary>
        /// Read all documents matching the provided prefix. Not all document sets will support this, those that do not will throw a NotSupportedException.
        /// </summary>
        /// <returns>
        /// An enumerable of documents.
        /// </returns>
        /// <exception cref="NotSupportedException">
        /// </exception>
        /// <remarks>
        /// </remarks>
        public IEnumerable<TDocument> GetAll()
        {
            if (this.CommonPrefixLocation == null)
            {
                throw new NotSupportedException();
            }

            return this.GetAllInternal(this.Blobs.ListBlobLocations(this.CommonPrefixLocation()));
        }

        /// <summary>
        /// Write the document. If it already exists, overwrite it.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void InsertOrReplace(TKey key, TDocument document)
        {
            var location = this.LocationOfKey(key);
            if (this.Blobs.PutBlob(location, document, true, this.Serializer))
            {
                this.SetCache(location, document);
            }
        }

        /// <summary>
        /// List the keys of all documents. Not all document sets will support this, those that do not will throw a NotSupportedException.
        /// </summary>
        /// <returns>
        /// An enumerable of keys.
        /// </returns>
        /// <exception cref="NotSupportedException">
        /// </exception>
        /// <remarks>
        /// </remarks>
        public virtual IEnumerable<TKey> ListAllKeys()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Try to read the document, if it exists.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <returns>
        /// The try get. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool TryGet(TKey key, out TDocument document)
        {
            var location = this.LocationOfKey(key);
            if (this.TryGetCache(location, out document))
            {
                return true;
            }

            var result = this.Blobs.GetBlob<TDocument>(location, this.Serializer);
            if (!result.HasValue)
            {
                document = default(TDocument);
                return false;
            }

            document = result.Value;
            this.SetCache(location, result.Value);
            return true;
        }

        /// <summary>
        /// Load the current document, or create a default document if it does not exist yet. Then update the document with the provided update function and persist the result.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="updateDocument">
        /// The update document. 
        /// </param>
        /// <param name="defaultIfNotExist">
        /// The default if not exist. 
        /// </param>
        /// <returns>
        /// A document.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TDocument Update(TKey key, Func<TDocument, TDocument> updateDocument, Func<TDocument> defaultIfNotExist)
        {
            var location = this.LocationOfKey(key);
            var document = this.Blobs.UpsertBlob(
                location, () => updateDocument(defaultIfNotExist()), updateDocument, this.Serializer);
            this.SetCache(location, document);
            return document;
        }

        /// <summary>
        /// If the document already exists, update it. If it does not exist yet, do nothing.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="updateDocument">
        /// The update document. 
        /// </param>
        /// <returns>
        /// A document.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TDocument UpdateIfExist(TKey key, Func<TDocument, TDocument> updateDocument)
        {
            var location = this.LocationOfKey(key);
            var result = this.Blobs.UpdateBlobIfExist(location, updateDocument, this.Serializer);
            if (!result.HasValue)
            {
                return default(TDocument);
            }

            this.SetCache(location, result.Value);
            return result.Value;
        }

        /// <summary>
        /// If the document already exists, update it with the provided update function. If the document does not exist yet, insert a new document with the provided insert function.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <param name="updateDocument">
        /// The update document. 
        /// </param>
        /// <param name="insertDocument">
        /// The insert document. 
        /// </param>
        /// <returns>
        /// A document.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TDocument UpdateOrInsert(
            TKey key, Func<TDocument, TDocument> updateDocument, Func<TDocument> insertDocument)
        {
            var location = this.LocationOfKey(key);
            var document = this.Blobs.UpsertBlob(location, insertDocument, updateDocument, this.Serializer);
            this.SetCache(location, document);
            return document;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Deletes all internal.
        /// </summary>
        /// <param name="prefix">
        /// The prefix. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected void DeleteAllInternal(IBlobLocation prefix)
        {
            this.RemoveCache(prefix);
            this.Blobs.DeleteAllBlobs(prefix);
        }

        /// <summary>
        /// Gets all internal.
        /// </summary>
        /// <param name="locations">
        /// The locations. 
        /// </param>
        /// <returns>
        /// An enumerable of documents.
        /// </returns>
        /// <remarks>
        /// </remarks>
        protected IEnumerable<TDocument> GetAllInternal(IEnumerable<IBlobLocation> locations)
        {
            foreach (var location in locations)
            {
                TDocument doc;
                if (this.TryGetCache(location, out doc))
                {
                    yield return doc;
                }
                else
                {
                    var blob = this.Blobs.GetBlob<TDocument>(location, this.Serializer);
                    if (blob.HasValue)
                    {
                        this.SetCache(location, blob.Value);
                        yield return blob.Value;
                    }
                }
            }
        }

        /// <summary>
        /// Override this method to plug in your cache provider, if needed. By default, no caching is performed.
        /// </summary>
        /// <param name="location">
        /// The location. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected virtual void RemoveCache(IBlobLocation location)
        {
        }

        /// <summary>
        /// Override this method to plug in your cache provider, if needed. By default, no caching is performed.
        /// </summary>
        /// <param name="location">
        /// The location. 
        /// </param>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected virtual void SetCache(IBlobLocation location, TDocument document)
        {
        }

        /// <summary>
        /// Override this method to plug in your cache provider, if needed. By default, no caching is performed.
        /// </summary>
        /// <param name="location">
        /// The location. 
        /// </param>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <returns>
        /// The try get cache. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        protected virtual bool TryGetCache(IBlobLocation location, out TDocument document)
        {
            document = default(TDocument);
            return false;
        }

        #endregion
    }
}