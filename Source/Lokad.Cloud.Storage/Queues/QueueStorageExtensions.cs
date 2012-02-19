#region Copyright (c) Lokad 2009-2010

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedMethodReturnValue.Global
namespace Lokad.Cloud.Storage.Queues
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// Helper extensions methods for storage providers.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public static class QueueStorageExtensions
    {
        #region Public Methods and Operators

        /// <summary>
        /// Clear all the messages from a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void Clear<T>(this IQueueStorageProvider provider)
        {
            provider.Clear(GetDefaultStorageName(typeof(T)));
        }

        /// <summary>
        /// Deletes a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <returns>
        /// <c>true</c> if the queue name has been actually deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static bool DeleteQueue<T>(this IQueueStorageProvider provider)
        {
            return provider.DeleteQueue(GetDefaultStorageName(typeof(T)));
        }

        /// <summary>
        /// Gets messages from a queue with a visibility timeout of 2 hours and a maximum of 50 processing trials.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// Provider for the queue storage. 
        /// </param>
        /// <param name="queueName">
        /// Identifier of the queue to be pulled. 
        /// </param>
        /// <param name="count">
        /// Maximal number of messages to be retrieved. 
        /// </param>
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<T> Get<T>(this IQueueStorageProvider provider, string queueName, int count)
        {
            return provider.Get<T>(queueName, count, new TimeSpan(2, 0, 0), 5);
        }

        /// <summary>
        /// Gets messages from a queue with a visibility timeout of 2 hours.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// Queue storage provider. 
        /// </param>
        /// <param name="queueName">
        /// Identifier of the queue to be pulled. 
        /// </param>
        /// <param name="count">
        /// Maximal number of messages to be retrieved. 
        /// </param>
        /// <param name="maxProcessingTrials">
        /// Maximum number of message processing trials, before the message is considered as being poisonous, removed from the queue and persisted to the 'failing-messages' store. 
        /// </param>
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<T> Get<T>(
            this IQueueStorageProvider provider, string queueName, int count, int maxProcessingTrials)
        {
            return provider.Get<T>(queueName, count, new TimeSpan(2, 0, 0), maxProcessingTrials);
        }

        /// <summary>
        /// Gets messages from a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
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
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<T> Get<T>(
            this IQueueStorageProvider provider, int count, TimeSpan visibilityTimeout, int maxProcessingTrials)
        {
            return provider.Get<T>(GetDefaultStorageName(typeof(T)), count, visibilityTimeout, maxProcessingTrials);
        }

        /// <summary>
        /// Gets messages from a queue (derived from the message type T) with a visibility timeout of 2 hours and a maximum of 50 processing trials.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// Provider for the queue storage. 
        /// </param>
        /// <param name="count">
        /// Maximal number of messages to be retrieved. 
        /// </param>
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<T> Get<T>(this IQueueStorageProvider provider, int count)
        {
            return provider.Get<T>(GetDefaultStorageName(typeof(T)), count, new TimeSpan(2, 0, 0), 5);
        }

        /// <summary>
        /// Gets messages from a queue (derived from the message type T) with a visibility timeout of 2 hours.
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// Queue storage provider. 
        /// </param>
        /// <param name="count">
        /// Maximal number of messages to be retrieved. 
        /// </param>
        /// <param name="maxProcessingTrials">
        /// Maximum number of message processing trials, before the message is considered as being poisonous, removed from the queue and persisted to the 'failing-messages' store. 
        /// </param>
        /// <returns>
        /// Enumeration of messages, possibly empty. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static IEnumerable<T> Get<T>(this IQueueStorageProvider provider, int count, int maxProcessingTrials)
        {
            return provider.Get<T>(GetDefaultStorageName(typeof(T)), count, new TimeSpan(2, 0, 0), maxProcessingTrials);
        }

        /// <summary>
        /// Gets the approximate number of items in a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <returns>
        /// The get approximate count.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static int GetApproximateCount<T>(this IQueueStorageProvider provider)
        {
            return provider.GetApproximateCount(GetDefaultStorageName(typeof(T)));
        }

        /// <summary>
        /// Gets the approximate age of the top message in a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Maybe<TimeSpan> GetApproximateLatency<T>(this IQueueStorageProvider provider)
        {
            return provider.GetApproximateLatency(GetDefaultStorageName(typeof(T)));
        }

        /// <summary>
        /// Gets the default name of the storage.
        /// </summary>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// The get default storage name.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static string GetDefaultStorageName(Type type)
        {
            Debug.Assert(type.FullName != null, "type.FullName != null");
            var name = type.FullName.ToLowerInvariant().Replace(".", "-");

            // TODO: need a smarter behavior with long type name.
            if (name.Length > 63)
            {
                throw new ArgumentOutOfRangeException("type", "Type name is too long for auto-naming.");
            }

            return name;
        }

        /// <summary>
        /// Gets the resilient.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="keepAliveAfter">
        /// The keep alive after. 
        /// </param>
        /// <param name="maxProcessingTrials">
        /// The max processing trials. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static KeepAliveMessageHandle<T> GetResilient<T>(
            this IQueueStorageProvider provider, string queueName, TimeSpan keepAliveAfter, int maxProcessingTrials)
            where T : class
        {
            var messages =
                provider.Get<T>(queueName, 1, keepAliveAfter + TimeSpan.FromSeconds(30), maxProcessingTrials).ToList();
            if (messages.Count == 0)
            {
                return null;
            }

            return new KeepAliveMessageHandle<T>(messages[0], provider, keepAliveAfter, TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Gets the resilient.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static KeepAliveMessageHandle<T> GetResilient<T>(this IQueueStorageProvider provider, string queueName)
            where T : class
        {
            return GetResilient<T>(provider, queueName, TimeSpan.FromSeconds(90), 5);
        }

        /// <summary>
        /// Gets the resilient.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="keepAliveAfter">
        /// The keep alive after. 
        /// </param>
        /// <param name="maxProcessingTrials">
        /// The max processing trials. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static KeepAliveMessageHandle<T> GetResilient<T>(
            this IQueueStorageProvider provider, TimeSpan keepAliveAfter, int maxProcessingTrials) where T : class
        {
            return GetResilient<T>(provider, GetDefaultStorageName(typeof(T)), keepAliveAfter, maxProcessingTrials);
        }

        /// <summary>
        /// Gets the resilient.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static KeepAliveMessageHandle<T> GetResilient<T>(this IQueueStorageProvider provider) where T : class
        {
            return GetResilient<T>(provider, GetDefaultStorageName(typeof(T)), TimeSpan.FromSeconds(90), 5);
        }

        /// <summary>
        /// Put a message on a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void Put<T>(this IQueueStorageProvider provider, T message)
        {
            provider.Put(GetDefaultStorageName(typeof(T)), message);
        }

        /// <summary>
        /// Put messages on a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="messages">
        /// Messages to be put. 
        /// </param>
        /// <remarks>
        /// If the queue does not exist, it gets created.
        /// </remarks>
        public static void PutRange<T>(this IQueueStorageProvider provider, IEnumerable<T> messages)
        {
            provider.PutRange(GetDefaultStorageName(typeof(T)), messages);
        }

        /// <summary>
        /// Put messages on a queue (derived from the message type T).
        /// </summary>
        /// <typeparam name="T">
        /// Type of the messages. 
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="messages">
        /// Messages to be put. 
        /// </param>
        /// <remarks>
        /// If the queue does not exist, it gets created.
        /// </remarks>
        public static void PutRangeParallel<T>(this IQueueStorageProvider provider, IEnumerable<T> messages)
        {
            provider.PutRangeParallel(GetDefaultStorageName(typeof(T)), messages);
        }

        /// <summary>
        /// Put a message on a queue (derived from the message type T), but keep it invisible for a delay.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="provider">
        /// The provider. 
        /// </param>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void PutWithDelay<T>(this IQueueStorageProvider provider, T message, TimeSpan delay)
        {
            provider.Put(GetDefaultStorageName(typeof(T)), message, delay: delay);
        }

        #endregion
    }
}