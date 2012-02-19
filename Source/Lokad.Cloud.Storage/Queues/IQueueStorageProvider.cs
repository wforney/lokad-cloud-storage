#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Queues
{
    using System;
    using System.Collections.Generic;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Abstraction of the Queue Storage.
    /// </summary>
    /// <remarks>
    /// This provider represents a <em>logical</em> queue, not the actual Queue Storage. In particular, the provider implementation deals with overflowing messages (that is to say messages larger than 8kb) on its own.
    /// </remarks>
    public interface IQueueStorageProvider
    {
        #region Public Methods and Operators

        /// <summary>
        /// Abandon a message being processed and put it visibly back on the queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the message. 
        /// </typeparam>
        /// <param name="message">
        /// Message to be abandoned. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <returns>
        /// <c>True</c> if the original message has been deleted. 
        /// </returns>
        /// <remarks>
        /// Message must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        bool Abandon<T>(T message, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        /// <summary>
        /// Abandon all messages still being processed. This is recommended to call e.g. when forcing a worker to shutting.
        /// </summary>
        /// <returns>
        /// The number of original messages actually deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        int AbandonAll();

        /// <summary>
        /// Abandon a set of messages being processed and put them visibly back on the queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="messages">
        /// Messages to be abandoned. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <returns>
        /// The number of original messages actually deleted. 
        /// </returns>
        /// <remarks>
        /// Messages must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        int AbandonRange<T>(
            IEnumerable<T> messages, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        /// <summary>
        /// Clear all the messages from the specified queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void Clear(string queueName);

        /// <summary>
        /// Deletes a message being processed from the queue.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <returns>
        /// <c>True</c> if the message has been deleted. 
        /// </returns>
        /// <remarks>
        /// Message must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        bool Delete<T>(T message);

        /// <summary>
        /// Delete a persisted message.
        /// </summary>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <param name="key">
        /// Unique key of the persisted message as returned by ListPersisted. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void DeletePersisted(string storeName, string key, IDataSerializer serializer = null);

        /// <summary>
        /// Deletes a queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the queue name has been actually deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        bool DeleteQueue(string queueName);

        /// <summary>
        /// Deletes messages being processed from the queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="messages">
        /// Messages to be removed. 
        /// </param>
        /// <returns>
        /// The number of messages actually deleted. 
        /// </returns>
        /// <remarks>
        /// Messages must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        int DeleteRange<T>(IEnumerable<T> messages);

        /// <summary>
        /// Gets messages from a queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="queueName">
        /// Identifier of the queue to be pulled. 
        /// </param>
        /// <param name="count">
        /// Maximal number of messages to be retrieved. 
        /// </param>
        /// <param name="visibilityTimeout">
        /// The visibility timeout, indicating when the not yet deleted message should become visible in the queue again. 
        /// </param>
        /// <param name="maxProcessingTrials">
        /// Maximum number of message processing trials, before the message is considered as being poisonous, removed from the queue and persisted to the 'failing-messages' store. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        IEnumerable<T> Get<T>(
            string queueName, 
            int count, 
            TimeSpan visibilityTimeout, 
            int maxProcessingTrials, 
            IDataSerializer serializer = null);

        /// <summary>
        /// Gets the approximate number of items in this queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// The get approximate count.
        /// </returns>
        /// <remarks>
        /// </remarks>
        int GetApproximateCount(string queueName);

        /// <summary>
        /// Gets the approximate age of the top message of this queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        Maybe<TimeSpan> GetApproximateLatency(string queueName);

        /// <summary>
        /// Get details of a persisted message for inspection and recovery.
        /// </summary>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <param name="key">
        /// Unique key of the persisted message as returned by ListPersisted. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        Maybe<PersistedMessage> GetPersisted(string storeName, string key, IDataSerializer serializer = null);

        /// <summary>
        /// Keep the message alive for another period.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <returns>
        /// The new visibility timeout 
        /// </returns>
        /// <remarks>
        /// </remarks>
        TimeSpan KeepAlive<T>(T message) where T : class;

        /// <summary>
        /// Gets the list of queues whose name start with the specified prefix.
        /// </summary>
        /// <param name="prefix">
        /// If <c>null</c> or empty, returns all queues. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        IEnumerable<string> List(string prefix);

        /// <summary>
        /// Enumerate the keys of all persisted messages of the provided store.
        /// </summary>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        IEnumerable<string> ListPersisted(string storeName);

        /// <summary>
        /// Persist a message being processed to a store and remove it from the queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the message. 
        /// </typeparam>
        /// <param name="message">
        /// Message to be persisted. 
        /// </param>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <param name="reason">
        /// Optional reason text on why the message has been taken out of the queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void Persist<T>(T message, string storeName, string reason);

        /// <summary>
        /// Persist a set of messages being processed to a store and remove them from the queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="messages">
        /// Messages to be persisted. 
        /// </param>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <param name="reason">
        /// Optional reason text on why the messages have been taken out of the queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void PersistRange<T>(IEnumerable<T> messages, string storeName, string reason);

        /// <summary>
        /// Put a message on a queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="queueName">
        /// Identifier of the queue where messages are put. 
        /// </param>
        /// <param name="message">
        /// Message to be put. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// If the queue does not exist, it gets created.
        /// </remarks>
        void Put<T>(
            string queueName, 
            T message, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null);

        /// <summary>
        /// Put messages on a queue.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="queueName">
        /// Identifier of the queue where messages are put. 
        /// </param>
        /// <param name="messages">
        /// Messages to be put. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// If the queue does not exist, it gets created.
        /// </remarks>
        void PutRange<T>(
            string queueName, 
            IEnumerable<T> messages, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null);

        /// <summary>
        /// Puts messages on a queue. Uses Tasks to increase thouroughput dramatically.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="queueName">
        /// Identifier of the queue where messages are put. 
        /// </param>
        /// <param name="messages">
        /// Messages to be put. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// If the queue does not exist, it gets created.
        /// </remarks>
        void PutRangeParallel<T>(
            string queueName, 
            IEnumerable<T> messages, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null);

        /// <summary>
        /// Put a persisted message back to the queue and delete it.
        /// </summary>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <param name="key">
        /// Unique key of the persisted message as returned by ListPersisted. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <remarks>
        /// </remarks>
        void RestorePersisted(
            string storeName, string key, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        /// <summary>
        /// Resume a message being processed later and put it visibly back on the queue, without decreasing the poison detection dequeue count.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the message. 
        /// </typeparam>
        /// <param name="message">
        /// Message to be resumed later. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <returns>
        /// <c>True</c> if the original message has been deleted. 
        /// </returns>
        /// <remarks>
        /// Message must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        bool ResumeLater<T>(T message, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        /// <summary>
        /// Resume a set of messages being processed latern and put them visibly back on the queue, without decreasing the poison detection dequeue count.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="messages">
        /// Messages to be resumed later. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <returns>
        /// The number of original messages actually deleted. 
        /// </returns>
        /// <remarks>
        /// Messages must have first been retrieved through <see cref="Get{T}"/> .
        /// </remarks>
        int ResumeLaterRange<T>(
            IEnumerable<T> messages, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        /// <summary>
        /// Revive messages that are no longer kept alive.
        /// </summary>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <returns>
        /// The revive messages.
        /// </returns>
        /// <remarks>
        /// </remarks>
        int ReviveMessages(TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan));

        #endregion
    }

    /// <summary>
    /// Persisted message details for inspection and recovery.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class PersistedMessage
    {
        #region Public Properties

        /// <summary>
        ///   XML representation of the message, if possible and supported by the serializer
        /// </summary>
        /// <remarks>
        /// </remarks>
        public Maybe<XElement> DataXml { get; internal set; }

        /// <summary>
        ///   The number of times the message has been dequeued.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public int DequeueCount { get; internal set; }

        /// <summary>
        ///   Time when the message was inserted into the message queue.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public DateTimeOffset InsertionTime { get; internal set; }

        /// <summary>
        ///   True if the raw message data is available and can be restored.
        /// </summary>
        /// <remarks>
        ///   Can be true even if DataXML is not available.
        /// </remarks>
        public bool IsDataAvailable { get; internal set; }

        /// <summary>
        ///   Unique key of the persisted message as returned by ListPersisted.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string Key { get; internal set; }

        /// <summary>
        ///   Time when the message was persisted and removed from the message queue.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public DateTimeOffset PersistenceTime { get; internal set; }

        /// <summary>
        ///   Identifier of the originating message queue.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string QueueName { get; internal set; }

        /// <summary>
        ///   Optional reason text why the message was persisted.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string Reason { get; internal set; }

        /// <summary>
        ///   Name of the message persistence store.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public string StoreName { get; internal set; }

        #endregion
    }
}