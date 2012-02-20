#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation
{
    /// <summary>
    /// The storage observer interface.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public interface IStorageObserver
    {
        #region Public Methods and Operators

        /// <summary>
        /// Notifies the specified storage event.
        /// </summary>
        /// <param name="storageEvent">The storage event.</param>
        /// <remarks></remarks>
        void Notify(IStorageEvent storageEvent);

        #endregion
    }
}