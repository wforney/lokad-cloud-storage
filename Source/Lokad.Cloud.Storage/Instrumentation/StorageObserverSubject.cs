#region Copyright (c) Lokad 2011-2012

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Instrumentation
{
    using System;

    /// <summary>
    /// Cloud storage observer that implements a hot Rx Observable, forwarding all events synchronously (similar to Rx's FastSubject). Use this class if you want an easy way to observe Lokad.Cloud.Storage using Rx. Alternatively you can implement your own storage observer instead, or not use any observers at all.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class StorageObserverSubject : IStorageObserver, IObservable<IStorageEvent>, IDisposable
    {
        #region Constants and Fields

        /// <summary>
        /// The fixed observers.
        /// </summary>
        private readonly IObserver<IStorageEvent>[] fixedObservers;

        /// <summary>
        /// The sync.
        /// </summary>
        private readonly object sync = new object();

        /// <summary>
        /// The is disposed.
        /// </summary>
        private bool isDisposed;

        /// <summary>
        /// The observers.
        /// </summary>
        private IObserver<IStorageEvent>[] observers;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageObserverSubject"/> class.
        /// </summary>
        /// <param name="fixedObservers">
        /// The fixed observers. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public StorageObserverSubject(IObserver<IStorageEvent>[] fixedObservers = null)
        {
            this.fixedObservers = fixedObservers ?? new IObserver<IStorageEvent>[0];
            this.observers = new IObserver<IStorageEvent>[0];
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public void Dispose()
        {
            lock (this.sync)
            {
                this.isDisposed = true;
                this.observers = null;
            }
        }

        /// <summary>
        /// Subscribes the specified observer.
        /// </summary>
        /// <param name="observer">
        /// The observer. 
        /// </param>
        /// <returns>
        /// A disposable interface.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IDisposable Subscribe(IObserver<IStorageEvent> observer)
        {
            if (this.isDisposed)
            {
                // make lifetime issues visible
                throw new ObjectDisposedException("StorageObserverSubject");
            }

            if (observer == null)
            {
                throw new ArgumentNullException("observer");
            }

            lock (this.sync)
            {
                var newObservers = new IObserver<IStorageEvent>[this.observers.Length + 1];
                Array.Copy(this.observers, newObservers, this.observers.Length);
                newObservers[this.observers.Length] = observer;
                this.observers = newObservers;
            }

            return new Subscription(this, observer);
        }

        #endregion

        #region Explicit Interface Methods

        /// <summary>
        /// Notifies the specified storage event.
        /// </summary>
        /// <param name="storageEvent">
        /// The storage event. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void IStorageObserver.Notify(IStorageEvent storageEvent)
        {
            if (this.isDisposed)
            {
                // make lifetime issues visible
                throw new ObjectDisposedException("StorageObserverSubject");
            }

            // Assuming storageEvent observers are light - else we may want to do this async
            foreach (var observer in this.fixedObservers)
            {
                observer.OnNext(storageEvent);
            }

            // assignment is atomic, no lock needed
            var observers1 = this.observers;
            foreach (var observer in observers1)
            {
                observer.OnNext(storageEvent);
            }
        }

        #endregion

        /// <summary>
        /// The subscription.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class Subscription : IDisposable
        {
            #region Constants and Fields

            /// <summary>
            /// The subject.
            /// </summary>
            private readonly StorageObserverSubject subject;

            /// <summary>
            /// The observer.
            /// </summary>
            private IObserver<IStorageEvent> observer;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="Subscription"/> class.
            /// </summary>
            /// <param name="subject">
            /// The subject. 
            /// </param>
            /// <param name="observer">
            /// The observer. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public Subscription(StorageObserverSubject subject, IObserver<IStorageEvent> observer)
            {
                this.subject = subject;
                this.observer = observer;
            }

            #endregion

            #region Public Methods and Operators

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public void Dispose()
            {
                if (this.observer != null)
                {
                    lock (this.subject.sync)
                    {
                        // Need to retest '_observer' because of race conditions 
                        if (this.observer != null && !this.subject.isDisposed)
                        {
                            var idx = Array.IndexOf(this.subject.observers, this.observer);
                            if (idx >= 0)
                            {
                                var newObservers = new IObserver<IStorageEvent>[this.subject.observers.Length + 1];
                                Array.Copy(this.subject.observers, 0, newObservers, 0, idx);
                                Array.Copy(
                                    this.subject.observers, 
                                    idx + 1, 
                                    newObservers, 
                                    idx, 
                                    this.subject.observers.Length - idx - 1);
                                this.subject.observers = newObservers;
                            }

                            this.observer = null;
                        }
                    }
                }
            }

            #endregion
        }
    }
}