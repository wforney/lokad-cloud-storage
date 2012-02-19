#region Copyright (c) Lokad 2009

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Queues
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The purpose of the <see cref="MessageEnvelope"/> is to provide additional metadata for a message.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    [Serializable]
    internal sealed class MessageEnvelope
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the dequeue count.
        /// </summary>
        /// <value> The dequeue count. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public int DequeueCount { get; set; }

        /// <summary>
        ///   Gets or sets the raw message.
        /// </summary>
        /// <value> The raw message. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public byte[] RawMessage { get; set; }

        #endregion
    }
}