#region Copyright (c) Lokad 2009

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System.IO;
    using System.Xml.Linq;

    /// <summary>
    /// Optional extension for custom formatters supporting an intermediate xml representation for inspection and recovery.
    /// </summary>
    /// <remarks>
    /// This extension can be implemented even when the serializer is not xml based but in a format that can be transformed to xml easily in a robust way (i.e. more robust than deserializing to a full object). Note that formatters should be registered in IoC as IBinaryFormatter, not by this extension interface.
    /// </remarks>
    public interface IIntermediateDataSerializer : IDataSerializer
    {
        #region Public Methods and Operators

        /// <summary>
        /// Transform and repack an object from xml to a stream.
        /// </summary>
        /// <param name="data">
        /// The data. 
        /// </param>
        /// <param name="destination">
        /// The destination. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void RepackXml(XElement data, Stream destination);

        /// <summary>
        /// Unpack and transform an object from a stream to xml.
        /// </summary>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <returns>
        /// The XML.
        /// </returns>
        /// <remarks>
        /// </remarks>
        XElement UnpackXml(Stream source);

        #endregion
    }
}