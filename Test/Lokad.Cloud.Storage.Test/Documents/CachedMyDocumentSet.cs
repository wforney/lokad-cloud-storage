#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Documents
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Runtime.Caching;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Documents;

    /// <summary>
    /// Simple document set
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class CachedMyDocumentSet : DocumentSet<MyDocument, int>
    {
        #region Constants and Fields

        /// <summary>
        /// The cache.
        /// </summary>
        private readonly MemoryCache cache;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CachedMyDocumentSet"/> class.
        /// </summary>
        /// <param name="blobs">
        /// The blobs. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public CachedMyDocumentSet(IBlobStorageProvider blobs)
            : base(blobs, key => new BlobLocation("document-container", key.ToString(CultureInfo.InvariantCulture)))
        {
            this.cache = MemoryCache.Default;
            this.Serializer = new CloudFormatter();
        }

        #endregion

        #region Methods

        /// <summary>
        /// Removes the cache.
        /// </summary>
        /// <param name="location">
        /// The location. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected override void RemoveCache(IBlobLocation location)
        {
            var prefix = location.ContainerName + "#" + location.Path;
            var items = this.cache.Where(p => p.Key.StartsWith(prefix)).ToList();
            foreach (var item in items)
            {
                this.cache.Remove(item.Key);
            }
        }

        /// <summary>
        /// Sets the cache.
        /// </summary>
        /// <param name="location">
        /// The location. 
        /// </param>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected override void SetCache(IBlobLocation location, MyDocument document)
        {
            this.cache.Set(
                location.ContainerName + "#" + location.Path, 
                document, 
                new CacheItemPolicy { SlidingExpiration = TimeSpan.FromMinutes(1) });
        }

        /// <summary>
        /// Tries the get cache.
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
        protected override bool TryGetCache(IBlobLocation location, out MyDocument document)
        {
            return null != (document = this.cache.Get(location.ContainerName + "#" + location.Path) as MyDocument);
        }

        #endregion
    }
}