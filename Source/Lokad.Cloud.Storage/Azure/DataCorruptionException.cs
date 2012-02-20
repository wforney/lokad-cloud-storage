#region Copyright (c) Lokad 2009

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception indicating that received data has been detected to be corrupt or altered.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [Serializable]
    public class DataCorruptionException : Exception
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DataCorruptionException"/> class. 
        /// </summary>
        /// <remarks>
        /// </remarks>
        public DataCorruptionException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataCorruptionException"/> class.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public DataCorruptionException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataCorruptionException"/> class.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="inner">
        /// The inner. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public DataCorruptionException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataCorruptionException"/> class. 
        /// </summary>
        /// <param name="info">
        /// The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown. 
        /// </param>
        /// <param name="context">
        /// The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination. 
        /// </param>
        /// <exception cref="T:System.ArgumentNullException">
        /// The
        ///   <paramref name="info"/>
        ///   parameter is null.
        /// </exception>
        /// <exception cref="T:System.Runtime.Serialization.SerializationException">
        /// The class name is null or
        ///   <see cref="P:System.Exception.HResult"/>
        ///   is zero (0).
        /// </exception>
        /// <remarks>
        /// </remarks>
        protected DataCorruptionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #endregion
    }
}