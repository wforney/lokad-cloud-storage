namespace Lokad.Cloud.Storage.Test.Blobs
{
    using System;

    /// <summary>
    /// The my blob.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [Serializable]
    internal class MyBlob
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MyBlob"/> class. 
        ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MyBlob()
        {
            this.MyGuid = Guid.NewGuid();
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets my GUID.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Guid MyGuid { get; private set; }

        #endregion
    }
}