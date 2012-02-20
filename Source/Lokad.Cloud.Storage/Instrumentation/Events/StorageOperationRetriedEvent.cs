#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation.Events
{
    using System;
    using System.Xml.Linq;

    /// <summary>
    /// Raised whenever a storage operation is retried. Useful for analyzing retry policy behavior.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class StorageOperationRetriedEvent : IStorageEvent
    {
        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageOperationRetriedEvent"/> class.
        /// </summary>
        /// <param name="exception">
        /// The exception. 
        /// </param>
        /// <param name="policy">
        /// The policy. 
        /// </param>
        /// <param name="trial">
        /// The trial. 
        /// </param>
        /// <param name="interval">
        /// The interval. 
        /// </param>
        /// <param name="trialSequence">
        /// The trial sequence. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public StorageOperationRetriedEvent(
            Exception exception, string policy, int trial, TimeSpan interval, Guid trialSequence)
        {
            this.Exception = exception;
            this.Policy = policy;
            this.Trial = trial;
            this.Interval = interval;
            this.TrialSequence = trialSequence;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the exception.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Exception Exception { get; private set; }

        /// <summary>
        ///   Gets the interval.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public TimeSpan Interval { get; private set; }

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
        ///   Gets the policy.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string Policy { get; private set; }

        /// <summary>
        ///   Gets the trial.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int Trial { get; private set; }

        /// <summary>
        ///   Gets the trial sequence.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Guid TrialSequence { get; private set; }

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
                "Storage: Operation was retried on policy {0} ({1} trial): {2}", 
                this.Policy, 
                this.Trial, 
                this.Exception != null ? this.Exception.Message : string.Empty);
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
                new XElement("Event", "StorageOperationRetriedEvent"));
        }

        #endregion
    }
}