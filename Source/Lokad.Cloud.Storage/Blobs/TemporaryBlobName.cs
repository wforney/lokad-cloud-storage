#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Reference to a unique blob with a fixed limited lifespan.
    /// </summary>
    /// <typeparam name="T">
    /// Type referred by the blob name. 
    /// </typeparam>
    /// <remarks>
    /// Used in conjunction with the Garbage Collector service. Use as base class for custom temporary blobs with additional attributes, or use the method <see cref="GetNew(System.DateTimeOffset)"/> to instantiate a new instance directly linked to the garbage collected container.
    /// </remarks>
    [Serializable]
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    public class TemporaryBlobName<T> : BlobName<T>
    {
        #region Constants and Fields

        /// <summary>
        ///   Name of the container for the temporary blobs.
        /// </summary>
        public const string DefaultContainerName = "lokad-cloud-temporary";

        /// <summary>
        ///   Define the time when the object becomes eligible for deletion.
        /// </summary>
        [Rank(0)]
        [DataMember]
        public readonly DateTimeOffset Expiration;

        /// <summary>
        ///   Suffix, provided to avoid collision between temporary blob name.
        /// </summary>
        [Rank(1)]
        [DataMember]
        public readonly string Suffix;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="TemporaryBlobName{T}"/> class. 
        /// Explicit constructor.
        /// </summary>
        /// <param name="expiration">
        /// The expiration. 
        /// </param>
        /// <param name="suffix">
        /// The suffix. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected TemporaryBlobName(DateTimeOffset expiration, string suffix)
        {
            this.Expiration = expiration;
            this.Suffix = suffix ?? this.GetType().FullName;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Returns the garbage collected container.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public override sealed string ContainerName
        {
            get
            {
                return DefaultContainerName;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Gets a full name to a temporary blob.
        /// </summary>
        /// <param name="expiration">
        /// The expiration. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static TemporaryBlobName<T> GetNew(DateTimeOffset expiration)
        {
            return new TemporaryBlobName<T>(expiration, Guid.NewGuid().ToString("N"));
        }

        /// <summary>
        /// Gets a full name to a temporary blob.
        /// </summary>
        /// <param name="expiration">
        /// The expiration. 
        /// </param>
        /// <param name="prefix">
        /// The prefix. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static TemporaryBlobName<T> GetNew(DateTimeOffset expiration, string prefix)
        {
            // hyphen used on purpose, not to interfere with parsing later on.
            return new TemporaryBlobName<T>(expiration, string.Format("{0}-{1}", prefix, Guid.NewGuid().ToString("N")));
        }

        #endregion
    }
}