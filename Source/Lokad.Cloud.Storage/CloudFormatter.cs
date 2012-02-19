#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Xml;
    using System.Xml.Linq;

    /// <summary>
    /// Formatter based on <c>DataContractSerializer</c> and <c>NetDataContractSerializer</c> . The formatter targets storage of persistent or transient data in the cloud storage.
    /// </summary>
    /// <remarks>
    /// If a <c>DataContract</c> attribute is present, then the <c>DataContractSerializer</c> is favored. If not, then the <c>NetDataContractSerializer</c> is used instead. This class is not <b>thread-safe</b> .
    /// </remarks>
    public class CloudFormatter : IIntermediateDataSerializer
    {
        #region Public Methods and Operators

        /// <summary>
        /// Deserializes the object from specified source stream.
        /// </summary>
        /// <param name="source">
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
        public object Deserialize(Stream source, Type type)
        {
            var serializer = GetXmlSerializer(type);

            using (var decompressed = Decompress(source, true))
            using (var reader = XmlDictionaryReader.CreateBinaryReader(decompressed, XmlDictionaryReaderQuotas.Max))
            {
                return serializer.ReadObject(reader);
            }
        }

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
        public void RepackXml(XElement data, Stream destination)
        {
            using (var compressed = Compress(destination, true))
            using (var writer = XmlDictionaryWriter.CreateBinaryWriter(compressed, null, null, false))
            {
                data.Save(writer);
                writer.Flush();
                compressed.Flush();
            }
        }

        /// <summary>
        /// Serializes the object to the specified stream.
        /// </summary>
        /// <param name="instance">
        /// The instance. 
        /// </param>
        /// <param name="destination">
        /// The destination stream. 
        /// </param>
        /// <param name="type">
        /// The type of the object to serialize (can be a base type of the provided instance). 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Serialize(object instance, Stream destination, Type type)
        {
            var serializer = GetXmlSerializer(type);

            using (var compressed = Compress(destination, true))
            using (var writer = XmlDictionaryWriter.CreateBinaryWriter(compressed, null, null, false))
            {
                serializer.WriteObject(writer, instance);
            }
        }

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
        public XElement UnpackXml(Stream source)
        {
            using (var decompressed = Decompress(source, true))
            using (var reader = XmlDictionaryReader.CreateBinaryReader(decompressed, XmlDictionaryReaderQuotas.Max))
            {
                return XElement.Load(reader);
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Compresses the specified stream.
        /// </summary>
        /// <param name="stream">
        /// The stream. 
        /// </param>
        /// <param name="leaveOpen">
        /// if set to <c>true</c> [leave open]. 
        /// </param>
        /// <returns>
        /// The GZip stream.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static GZipStream Compress(Stream stream, bool leaveOpen)
        {
            return new GZipStream(stream, CompressionMode.Compress, leaveOpen);
        }

        /// <summary>
        /// Decompresses the specified stream.
        /// </summary>
        /// <param name="stream">
        /// The stream. 
        /// </param>
        /// <param name="leaveOpen">
        /// if set to <c>true</c> [leave open]. 
        /// </param>
        /// <returns>
        /// The GZip stream.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static GZipStream Decompress(Stream stream, bool leaveOpen)
        {
            return new GZipStream(stream, CompressionMode.Decompress, leaveOpen);
        }

        /// <summary>
        /// Retrieve attributes from the type.
        /// </summary>
        /// <typeparam name="T">
        /// Attribute to use 
        /// </typeparam>
        /// <param name="target">
        /// Type to perform operation upon 
        /// </param>
        /// <param name="inherit">
        /// <see cref="MemberInfo.GetCustomAttributes(Type,bool)"/> 
        /// </param>
        /// <returns>
        /// Empty array of <typeparamref name="T"/> if there are no attributes 
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static T[] GetAttributes<T>(ICustomAttributeProvider target, bool inherit) where T : Attribute
        {
            return target.IsDefined(typeof(T), inherit)
                       ? target.GetCustomAttributes(typeof(T), inherit).Select(a => (T)a).ToArray()
                       : new T[0];
        }

        /// <summary>
        /// Gets the XML serializer.
        /// </summary>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// The XML object serializer.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static XmlObjectSerializer GetXmlSerializer(Type type)
        {
            // 'false' == do not inherit the attribute
            if (GetAttributes<DataContractAttribute>(type, false).Length > 0)
            {
                return new DataContractSerializer(type);
            }

            return new NetDataContractSerializer();
        }

        #endregion
    }
}