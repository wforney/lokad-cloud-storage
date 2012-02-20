#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation
{
    /// <summary>
    /// The storage event level.
    /// </summary>
    public enum StorageEventLevel
    {
        /// <summary>
        /// The trace.
        /// </summary>
        Trace = 1, 

        /// <summary>
        /// The information.
        /// </summary>
        Information = 2, 

        /// <summary>
        /// The warning.
        /// </summary>
        Warning = 3, 

        /// <summary>
        /// The error.
        /// </summary>
        Error = 4, 

        /// <summary>
        /// The fatal error.
        /// </summary>
        FatalError = 5
    }
}