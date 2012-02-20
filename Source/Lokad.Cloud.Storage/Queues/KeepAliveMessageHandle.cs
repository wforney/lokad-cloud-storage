#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Queues
{
    using System;
    using System.Threading;

    /// <summary>
    /// The keep alive message handle of type T.
    /// </summary>
    /// <typeparam name="T">
    /// The type. 
    /// </typeparam>
    public class KeepAliveMessageHandle<T> : IDisposable
        where T : class
    {
        #region Constants and Fields

        /// <summary>
        ///   The storage.
        /// </summary>
        private readonly IQueueStorageProvider storage;

        /// <summary>
        ///   The timer.
        /// </summary>
        private readonly Timer timer;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="KeepAliveMessageHandle{T}"/> class. 
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="storage">
        /// The storage. 
        /// </param>
        /// <param name="keepAliveAfter">
        /// The keep alive after. 
        /// </param>
        /// <param name="keepAlivePeriod">
        /// The keep alive period. 
        /// </param>
        public KeepAliveMessageHandle(
            T message, IQueueStorageProvider storage, TimeSpan keepAliveAfter, TimeSpan keepAlivePeriod)
        {
            this.storage = storage;
            this.Message = message;

            this.timer = new Timer(state => this.storage.KeepAlive(this.Message), null, keepAliveAfter, keepAlivePeriod);
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the message.
        /// </summary>
        public T Message { get; private set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Abandons this instance.
        /// </summary>
        public void Abandon()
        {
            this.storage.Abandon(this.Message);
        }

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        public void Delete()
        {
            this.storage.Delete(this.Message);
        }

        /// <summary>
        /// Resumes the later.
        /// </summary>
        public void ResumeLater()
        {
            this.storage.ResumeLater(this.Message);
        }

        #endregion

        #region Explicit Interface Methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        void IDisposable.Dispose()
        {
            this.timer.Dispose();
            this.storage.Abandon(this.Message);
        }

        #endregion
    }
}