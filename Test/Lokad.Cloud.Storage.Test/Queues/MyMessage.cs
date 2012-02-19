namespace Lokad.Cloud.Storage.Test.Queues
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The my message.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract]
    public class MyMessage
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MyMessage"/> class. 
        ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MyMessage()
        {
            this.MyGuid = Guid.NewGuid();
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets or sets my buffer.
        /// </summary>
        /// <value> My buffer. </value>
        /// <remarks>
        /// </remarks>
        [DataMember]
        public byte[] MyBuffer { get; set; }

        /// <summary>
        ///   Gets my GUID.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [DataMember(IsRequired = false)]
        public Guid MyGuid { get; private set; }

        #endregion
    }
}