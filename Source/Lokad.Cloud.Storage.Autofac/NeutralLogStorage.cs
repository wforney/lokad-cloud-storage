#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Autofac
{
    using Lokad.Cloud.Storage.Blobs;

    /// <summary>
    /// Storage for logging that do not log themselves (breaking potential cycles)
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class NeutralLogStorage
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the BLOB storage.
        /// </summary>
        /// <value> The BLOB storage. </value>
        /// <remarks>
        /// </remarks>
        public IBlobStorageProvider BlobStorage { get; set; }

        #endregion
    }
}