#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    using System;
    using System.Xml.Linq;

    /// <summary>
    /// Raised whenever a message is quarantined because it failed to be processed multiple times.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class MessageProcessingFailedQuarantinedEvent : IStorageEvent
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageProcessingFailedQuarantinedEvent"/> class.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <param name="messageType">
        /// Type of the message. 
        /// </param>
        /// <param name="data">
        /// The data. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public MessageProcessingFailedQuarantinedEvent(
            string queueName, string storeName, Type messageType, byte[] data)
        {
            this.QueueName = queueName;
            this.QuarantineStoreName = storeName;
            this.MessageType = messageType;
            this.Data = data;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the data.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public byte[] Data { get; private set; }

        /// <summary>
        ///   Gets the level.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public StorageEventLevel Level
        {
            get
            {
                return StorageEventLevel.Warning;
            }
        }

        /// <summary>
        ///   Gets the type of the message.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Type MessageType { get; private set; }

        /// <summary>
        ///   Gets the name of the quarantine store.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string QuarantineStoreName { get; private set; }

        /// <summary>
        ///   Gets the name of the queue.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string QueueName { get; private set; }

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
                    "Storage: A message of type {0} in queue {1} failed to process repeatedly and has been quarantined.", 
                    this.MessageType.Name, 
                    this.QueueName);
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
            return new XElement(
                "Meta", 
                new XElement("Component", "Lokad.Cloud.Storage"), 
                new XElement("Event", "MessageProcessingFailedQuarantinedEvent"));
        }

        #endregion
    }
}