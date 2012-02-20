#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    using System;
    using System.Xml.Linq;

    /// <summary>
    /// Raised whenever a blob is ignored because it could not be deserialized. Useful to monitor for serialization and data transport errors, alarm when it happens to often.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class BlobDeserializationFailedEvent : IStorageEvent
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobDeserializationFailedEvent"/> class.
        /// </summary>
        /// <param name="exception">
        /// The exception. 
        /// </param>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public BlobDeserializationFailedEvent(Exception exception, string containerName, string blobName)
        {
            this.Exception = exception;
            this.ContainerName = containerName;
            this.BlobName = blobName;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the name of the BLOB.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string BlobName { get; private set; }

        /// <summary>
        ///   Gets the name of the container.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string ContainerName { get; private set; }

        /// <summary>
        ///   Gets the exception.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Exception Exception { get; private set; }

        /// <summary>
        ///   Gets the level.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public StorageEventLevel Level
        {
            get
            {
                return StorageEventLevel.Error;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Describes this instance.
        /// </summary>
        /// <returns>
        /// The describe.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public string Describe()
        {
            return
                string.Format(
                    "Storage: A blob was retrieved but failed to deserialize. The blob was ignored. Blob {0} in container {1}. Reason: {2}", 
                    this.BlobName, 
                    this.ContainerName, 
                    this.Exception != null ? this.Exception.Message : "unknown");
        }

        /// <summary>
        /// Describes the meta.
        /// </summary>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public XElement DescribeMeta()
        {
            var meta = new XElement(
                "Meta", 
                new XElement("Component", "Lokad.Cloud.Storage"), 
                new XElement("Event", "BlobDeserializationFailedEvent"));

            if (this.Exception != null)
            {
                meta.Add(
                    new XElement(
                        "Exception", 
                        new XAttribute("typeName", this.Exception.GetType().FullName), 
                        new XAttribute("message", this.Exception.Message), 
                        this.Exception.ToString()));
            }

            return meta;
        }

        #endregion
    }
}