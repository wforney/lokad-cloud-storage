#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    /// <summary>
    /// Blob reference, to be used a short hand while operating with the <see cref="IBlobStorageProvider"/>
    /// </summary>
    public interface IBlobLocation
    {
        #region Public Properties

        /// <summary>
        ///   Gets the name of the container where the blob is located.
        /// </summary>
        string ContainerName { get; }

        /// <summary>
        ///   Gets the location of the blob inside of the container.
        /// </summary>
        string Path { get; }

        #endregion
    }
}