#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation
{
    using System.Xml.Linq;

    /// <summary>
    /// The i storage event.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public interface IStorageEvent
    {
        #region Public Properties

        /// <summary>
        ///   Gets the level.
        /// </summary>
        /// <remarks>
        /// </remarks>
        StorageEventLevel Level { get; }

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
        string Describe();

        /// <summary>
        /// Describes the meta.
        /// </summary>
        /// <returns>
        /// The XML.
        /// </returns>
        /// <remarks>
        /// </remarks>
        XElement DescribeMeta();

        #endregion
    }
}