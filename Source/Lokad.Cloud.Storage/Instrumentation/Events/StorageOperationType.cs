#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    /// <summary>
    /// The storage operation type.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public enum StorageOperationType
    {
        /// <summary>
        /// The blob put.
        /// </summary>
        BlobPut, 

        /// <summary>
        /// The blob get.
        /// </summary>
        BlobGet, 

        /// <summary>
        /// The blob get if modified.
        /// </summary>
        BlobGetIfModified, 

        /// <summary>
        /// The blob upsert or skip.
        /// </summary>
        BlobUpsertOrSkip, 

        /// <summary>
        /// The blob delete.
        /// </summary>
        BlobDelete, 

        /// <summary>
        /// The table query.
        /// </summary>
        TableQuery, 

        /// <summary>
        /// The table insert.
        /// </summary>
        TableInsert, 

        /// <summary>
        /// The table update.
        /// </summary>
        TableUpdate, 

        /// <summary>
        /// The table delete.
        /// </summary>
        TableDelete, 

        /// <summary>
        /// The table upsert.
        /// </summary>
        TableUpsert, 

        /// <summary>
        /// The queue get.
        /// </summary>
        QueueGet, 

        /// <summary>
        /// The queue put.
        /// </summary>
        QueuePut, 

        /// <summary>
        /// The queue delete.
        /// </summary>
        QueueDelete, 

        /// <summary>
        /// The queue abandon.
        /// </summary>
        QueueAbandon, 

        /// <summary>
        /// The queue persist.
        /// </summary>
        QueuePersist, 

        /// <summary>
        /// The queue wrap.
        /// </summary>
        QueueWrap, 

        /// <summary>
        /// The queue unwrap.
        /// </summary>
        QueueUnwrap
    }
}