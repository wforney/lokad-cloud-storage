#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Documents
{
    using System;
    using System.IO;
    using System.IO.Compression;

    using Lokad.Cloud.Storage.Blobs;

    /// <summary>
    /// Base class for a set of documents to be serialized using a BinaryWriter
    /// </summary>
    /// <typeparam name="TDocument">
    /// The type of the document. 
    /// </typeparam>
    /// <typeparam name="TKey">
    /// The type of the key. 
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    public abstract class CompressedBinaryDocumentSet<TDocument, TKey> : DocumentSet<TDocument, TKey>, IDataSerializer
        where TDocument : class
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CompressedBinaryDocumentSet{TDocument,TKey}"/> class. 
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
        /// <remarks>
        /// </remarks>
        protected CompressedBinaryDocumentSet(
            IBlobStorageProvider blobs, Func<TKey, IBlobLocation> locationOfKey, Func<IBlobLocation> commonPrefix = null)
            : base(blobs, locationOfKey, commonPrefix)
        {
            this.Serializer = this;
        }

        #endregion

        #region Explicit Interface Methods

        /// <summary>
        /// Deserializes the object from specified source stream.
        /// </summary>
        /// <param name="sourceStream">
        /// The source stream. 
        /// </param>
        /// <param name="type">
        /// The type of the object to deserialize. 
        /// </param>
        /// <returns>
        /// deserialized object 
        /// </returns>
        /// <remarks>
        /// </remarks>
        object IDataSerializer.Deserialize(Stream sourceStream, Type type)
        {
            using (var decompressed = new GZipStream(sourceStream, CompressionMode.Decompress, true))
            using (var reader = new BinaryReader(decompressed))
            {
                return this.Deserialize(reader);
            }
        }

        /// <summary>
        /// Serializes the object to the specified stream.
        /// </summary>
        /// <param name="instance">
        /// The instance. 
        /// </param>
        /// <param name="destinationStream">
        /// The destination stream. 
        /// </param>
        /// <param name="type">
        /// The type of the object to serialize (can be a base type of the provided instance). 
        /// </param>
        /// <remarks>
        /// </remarks>
        void IDataSerializer.Serialize(object instance, Stream destinationStream, Type type)
        {
            var document = instance as TDocument;
            if (document == null)
            {
                throw new NotSupportedException();
            }

            using (var compressed = new GZipStream(destinationStream, CompressionMode.Compress, true))
            using (var buffered = new BufferedStream(compressed, 4 * 1024))
            using (var writer = new BinaryWriter(buffered))
            {
                this.Serialize(document, writer);

                writer.Flush();
                buffered.Flush();
                compressed.Flush();
                compressed.Close();
            }
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
        /// The document.
        /// </returns>
        /// <remarks>
        /// </remarks>
        protected abstract TDocument Deserialize(BinaryReader reader);

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
        protected abstract void Serialize(TDocument document, BinaryWriter writer);

        #endregion
    }
}