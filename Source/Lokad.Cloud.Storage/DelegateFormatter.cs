#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.IO;

    /// <summary>
    /// Delegate formatter for ad-hoc scenarios
    /// </summary>
    public class DelegateFormatter : IDataSerializer
    {
        #region Constants and Fields

        /// <summary>
        /// The deserialize.
        /// </summary>
        private readonly Func<Type, Stream, object> deserialize;

        /// <summary>
        /// The serialize.
        /// </summary>
        private readonly Action<object, Type, Stream> serialize;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateFormatter"/> class.
        /// </summary>
        /// <param name="serialize">The serialize.</param>
        /// <param name="deserialize">The deserialize.</param>
        /// <remarks></remarks>
        public DelegateFormatter(Action<object, Type, Stream> serialize, Func<Type, Stream, object> deserialize)
        {
            this.serialize = serialize;
            this.deserialize = deserialize;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Deserializes the object from specified source stream.
        /// </summary>
        /// <param name="sourceStream">The source stream.</param>
        /// <param name="type">The type of the object to deserialize.</param>
        /// <returns>deserialized object</returns>
        /// <remarks></remarks>
        public object Deserialize(Stream sourceStream, Type type)
        {
            return this.deserialize(type, sourceStream);
        }

        /// <summary>
        /// Serializes the object to the specified stream.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <param name="destinationStream">The destination stream.</param>
        /// <param name="type">The type of the object to serialize (can be a base type of the provided instance).</param>
        /// <remarks></remarks>
        public void Serialize(object instance, Stream destinationStream, Type type)
        {
            this.serialize(instance, type, destinationStream);
        }

        #endregion
    }
}