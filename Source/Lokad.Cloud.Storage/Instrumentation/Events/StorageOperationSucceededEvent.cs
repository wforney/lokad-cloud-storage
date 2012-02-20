#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    using System;
    using System.Xml.Linq;

    /// <summary>
    /// Raised whenever a storage operation has succeeded. Useful for collecting usage statistics.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class StorageOperationSucceededEvent : IStorageEvent
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageOperationSucceededEvent"/> class.
        /// </summary>
        /// <param name="operationType">
        /// Type of the operation. 
        /// </param>
        /// <param name="duration">
        /// The duration. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public StorageOperationSucceededEvent(StorageOperationType operationType, TimeSpan duration)
        {
            this.OperationType = operationType;
            this.Duration = duration;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the duration.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public TimeSpan Duration { get; private set; }

        /// <summary>
        ///   Gets the level.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public StorageEventLevel Level
        {
            get
            {
                return StorageEventLevel.Trace;
            }
        }

        /// <summary>
        ///   Gets the type of the operation.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public StorageOperationType OperationType { get; private set; }

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
                "Storage: {0} operation succeeded in {1:0.00}s", this.OperationType, this.Duration.TotalSeconds);
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
                new XElement("Event", "StorageOperationSucceededEvent"));
        }

        #endregion
    }
}