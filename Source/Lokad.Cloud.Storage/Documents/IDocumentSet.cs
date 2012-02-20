#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Documents
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents a set of documents and how they are persisted.
    /// </summary>
    /// <typeparam name="TDocument">
    /// The type of the T document. 
    /// </typeparam>
    /// <typeparam name="TKey">
    /// The type of the T key. 
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    public interface IDocumentSet<TDocument, TKey>
    {
        #region Public Methods and Operators

        /// <summary>
        /// Delete all document matching the provided prefix. Not all document sets will support this, those that do not will throw a NotSupportedException.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// </exception>
        /// <remarks>
        /// </remarks>
        void DeleteAll();

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
        bool DeleteIfExist(TKey key);

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
        IEnumerable<TDocument> GetAll();

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
        void InsertOrReplace(TKey key, TDocument document);

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
        IEnumerable<TKey> ListAllKeys();

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
        bool TryGet(TKey key, out TDocument document);

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
        TDocument Update(TKey key, Func<TDocument, TDocument> updateDocument, Func<TDocument> defaultIfNotExist);

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
        TDocument UpdateIfExist(TKey key, Func<TDocument, TDocument> updateDocument);

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
        TDocument UpdateOrInsert(TKey key, Func<TDocument, TDocument> updateDocument, Func<TDocument> insertDocument);

        #endregion
    }
}