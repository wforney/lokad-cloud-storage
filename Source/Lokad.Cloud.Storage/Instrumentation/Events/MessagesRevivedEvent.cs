#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Xml.Linq;

    /// <summary>
    /// Raised whenever one or more messages have been revived (e.g. from kee-alive messages that were no longer kept alive).
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class MessagesRevivedEvent : IStorageEvent
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MessagesRevivedEvent"/> class.
        /// </summary>
        /// <param name="messageCountByQueueName">
        /// Name of the message count by queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public MessagesRevivedEvent(Dictionary<string, int> messageCountByQueueName)
        {
            this.MessageCountByQueueName = messageCountByQueueName;
        }

        #endregion

        #region Public Properties

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
        ///   Gets the name of the message count by queue.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Dictionary<string, int> MessageCountByQueueName { get; private set; }

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
            return string.Format(
                "Storage: Messages have been revived: {0}.",
                string.Join(
                    ", ", this.MessageCountByQueueName.Select(p => string.Format("{0} from {1}", p.Value, p.Key))));
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
                "Meta", new XElement("Component", "Lokad.Cloud.Storage"), new XElement("Event", "MessagesRevivedEvent"));
        }

        #endregion
    }
}