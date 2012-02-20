#region Copyright (c) Lokad 2009-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Autofac
{
    using System.Linq;

    using global::Autofac;

    using global::Autofac.Core;

    using global::Autofac.Core.Registration;

    using Lokad.Cloud.Storage.Blobs;

    /// <summary>
    /// Verifies that storage credentials are correct and allow access to blob and queue storage.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class StorageCredentialsVerifier
    {
        #region Constants and Fields

        /// <summary>
        /// The storage.
        /// </summary>
        private readonly IBlobStorageProvider storage;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the StorageCredentialsVerifier class.
        /// </summary>
        /// <param name="container">
        /// The container. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public StorageCredentialsVerifier(IContainer container)
        {
            try
            {
                this.storage = container.Resolve<IBlobStorageProvider>();
            }
            catch (ComponentNotRegisteredException)
            {
            }
            catch (DependencyResolutionException)
            {
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Verifies the storage credentials.
        /// </summary>
        /// <returns>
        /// <c>true</c> if the credentials are correct, <c>false</c> otherwise. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool VerifyCredentials()
        {
            if (this.storage == null)
            {
                return false;
            }

            try
            {
                // It is necssary to enumerate in order to actually send the request
                this.storage.ListContainers().ToList();

                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion
    }
}