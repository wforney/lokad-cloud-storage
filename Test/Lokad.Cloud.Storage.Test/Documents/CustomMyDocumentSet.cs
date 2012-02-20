#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Documents
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Documents;

    /// <summary>
    /// Full custom document set with compressed BinaryWriter serialization
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class CustomMyDocumentSet : CompressedBinaryDocumentSet<MyDocument, int>
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CustomMyDocumentSet"/> class.
        /// </summary>
        /// <param name="blobs">
        /// The blobs. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public CustomMyDocumentSet(IBlobStorageProvider blobs)
            : base(blobs, KeyToLocation, () => new BlobLocation("document-container", string.Empty))
        {
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Lists all keys.
        /// </summary>
        /// <returns>
        /// The keys.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override IEnumerable<int> ListAllKeys()
        {
            return this.Blobs.ListBlobNames("document-container").Select(int.Parse);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Deserializes the specified reader.
        /// </summary>
        /// <param name="reader">
        /// The reader. 
        /// </param>
        /// <returns>
        /// My document.
        /// </returns>
        /// <remarks>
        /// </remarks>
        protected override MyDocument Deserialize(BinaryReader reader)
        {
            var textBytesCount = reader.ReadInt32();
            var textBytes = reader.ReadBytes(textBytesCount);
            var text = Encoding.UTF8.GetString(textBytes);
            return new MyDocument { ArbitraryString = text };
        }

        /// <summary>
        /// Serializes the specified document.
        /// </summary>
        /// <param name="document">
        /// The document. 
        /// </param>
        /// <param name="writer">
        /// The writer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected override void Serialize(MyDocument document, BinaryWriter writer)
        {
            if (string.IsNullOrEmpty(document.ArbitraryString))
            {
                writer.Write(0);
            }
            else
            {
                var stringBytes = Encoding.UTF8.GetBytes(document.ArbitraryString);
                writer.Write(stringBytes.Length);
                writer.Write(stringBytes);
            }
        }

        /// <summary>
        /// Keys to location.
        /// </summary>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <returns>
        /// The BLOB location interface.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static IBlobLocation KeyToLocation(int key)
        {
            return new BlobLocation("document-container", key.ToString(CultureInfo.InvariantCulture));
        }

        #endregion
    }
}