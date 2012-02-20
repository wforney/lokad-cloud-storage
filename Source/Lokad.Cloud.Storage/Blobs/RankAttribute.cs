#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;

    /// <summary>
    /// Used to specify the field position in the blob name.
    /// </summary>
    /// <remarks>
    /// The name (chosen as the abbreviation of "field position") is made compact not to make client code too verbose.
    /// </remarks>
    public class RankAttribute : Attribute
    {
        #region Constants and Fields

        /// <summary>
        ///   Index of the property within the generated blob name.
        /// </summary>
        public readonly int Index;

        /// <summary>
        ///   Indicates whether the default value (for value types) should be treated as 'null'. Not relevant for class types.
        /// </summary>
        public readonly bool TreatDefaultAsNull;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="RankAttribute"/> class. 
        /// Position v
        /// </summary>
        /// <param name="index">
        /// The index. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public RankAttribute(int index)
        {
            this.Index = index;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RankAttribute"/> class. 
        /// Position v, and default behavior.
        /// </summary>
        /// <param name="index">
        /// The index. 
        /// </param>
        /// <param name="treatDefaultAsNull">
        /// if set to <c>true</c> [treat default as null]. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public RankAttribute(int index, bool treatDefaultAsNull)
        {
            this.Index = index;
            this.TreatDefaultAsNull = treatDefaultAsNull;
        }

        #endregion
    }
}