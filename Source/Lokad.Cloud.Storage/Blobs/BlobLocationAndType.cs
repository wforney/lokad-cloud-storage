#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Blob reference, to be used a short hand while operating with the <see cref="IBlobStorageProvider"/>
    /// </summary>
    /// <typeparam name="T">
    /// The type of BLOB.
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    [Serializable]
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    public class BlobLocationAndType<T> : IBlobLocationAndType<T>
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLocationAndType{T}"/> class.
        /// </summary>
        /// <param name="containerName">
        /// Name of the container. 
        /// </param>
        /// <param name="path">
        /// The path. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public BlobLocationAndType(string containerName, string path)
        {
            this.ContainerName = containerName;
            this.Path = path;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLocationAndType{T}"/> class, pointing to the same location (copy) as the provided location.
        /// </summary>
        /// <param name="fromLocation">
        /// From location. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public BlobLocationAndType(IBlobLocation fromLocation)
        {
            this.ContainerName = fromLocation.ContainerName;
            this.Path = fromLocation.Path;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the name of the container where the blob is located.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public string ContainerName { get; private set; }

        /// <summary>
        ///   Gets the location of the blob inside of the container.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public string Path { get; private set; }

        #endregion
    }
}