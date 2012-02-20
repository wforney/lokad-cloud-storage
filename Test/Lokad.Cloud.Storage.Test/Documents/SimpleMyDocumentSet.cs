#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Documents
{
    using System.Globalization;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Documents;

    /// <summary>
    /// Simple document set
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class SimpleMyDocumentSet : DocumentSet<MyDocument, int>
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleMyDocumentSet"/> class.
        /// </summary>
        /// <param name="blobs">
        /// The blobs. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public SimpleMyDocumentSet(IBlobStorageProvider blobs)
            : base(
                blobs, 
                key => new BlobLocation("document-container", key.ToString(CultureInfo.InvariantCulture)), 
                () => new BlobLocation("document-container", string.Empty), 
                new CloudFormatter())
        {
        }

        #endregion
    }
}