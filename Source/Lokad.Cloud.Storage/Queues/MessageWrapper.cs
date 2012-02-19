#region Copyright (c) Lokad 2009

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Queues
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The purpose of the <see cref="MessageWrapper"/> is to gracefully handle messages that are too large of the queue storage (or messages that happen to be already stored in the Blob Storage).
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    [Serializable]
    internal sealed class MessageWrapper
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the name of the BLOB.
        /// </summary>
        /// <value> The name of the BLOB. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public string BlobName { get; set; }

        /// <summary>
        ///   Gets or sets the name of the container.
        /// </summary>
        /// <value> The name of the container. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public string ContainerName { get; set; }

        #endregion
    }
}