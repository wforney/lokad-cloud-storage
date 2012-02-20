#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Documents
{
    using System.Globalization;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Documents;
    using Lokad.Cloud.Storage.InMemory;

    using NUnit.Framework;

    /// <summary>
    /// The adhoc document set tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public class AdhocDocumentSetTests : DocumentSetTests
    {
        #region Methods

        /// <summary>
        /// Builds the document set.
        /// </summary>
        /// <returns>
        /// A document set.
        /// </returns>
        /// <remarks>
        /// </remarks>
        protected override IDocumentSet<MyDocument, int> BuildDocumentSet()
        {
            var blobs = new MemoryBlobStorageProvider();
            return new DocumentSet<MyDocument, int>(
                blobs, i => new BlobLocation("container", i.ToString(CultureInfo.InvariantCulture)));
        }

        #endregion
    }
}