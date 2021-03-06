﻿#region (c)2009-2011 Lokad - New BSD license

// Company: http://www.lokad.com
// This code is released under the terms of the new BSD licence
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.IO;

    /// <summary>
    /// Generic data serializer interface.
    /// </summary>
    public interface IDataSerializer
    {
        #region Public Methods and Operators

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
        object Deserialize(Stream sourceStream, Type type);

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
        void Serialize(object instance, Stream destinationStream, Type type);

        #endregion
    }
}