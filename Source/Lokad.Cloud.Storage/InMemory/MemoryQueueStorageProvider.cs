#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Shared.Monads;

    using Tu = System.Tuple<string, object, System.Collections.Generic.List<byte[]>>;

    /// <summary>
    /// Mock in-memory Queue Storage.
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class MemoryQueueStorageProvider : IQueueStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        /// The in process messages.
        /// </summary>
        private readonly Dictionary<object, Tu> inProcessMessages;

        /// <summary>
        /// The persisted messages.
        /// </summary>
        private readonly HashSet<Tuple<string, string, string, byte[]>> persistedMessages;

        /// <summary>
        /// The queues.
        /// </summary>
        private readonly Dictionary<string, Queue<byte[]>> queues;

        /// <summary>
        ///   Root used to synchronize accesses to <c>_inprocess</c> .
        /// </summary>
        private readonly object sync = new object();

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryQueueStorageProvider"/> class. 
        ///   Default constructor.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryQueueStorageProvider()
        {
            this.queues = new Dictionary<string, Queue<byte[]>>();
            this.inProcessMessages = new Dictionary<object, Tu>();
            this.persistedMessages = new HashSet<Tuple<string, string, string, byte[]>>();
            this.DefaultSerializer = new CloudFormatter();
        }

        #endregion

        #region Properties

        /// <summary>
        ///   Gets or sets the default serializer.
        /// </summary>
        /// <value> The default serializer. </value>
        /// <remarks>
        /// </remarks>
        internal IDataSerializer DefaultSerializer { get; set; }

        #endregion

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
        /// </remarks>
        public bool Abandon<T>(T message, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            lock (this.sync)
            {
                Tu inProcess;
                if (!this.inProcessMessages.TryGetValue(message, out inProcess))
                {
                    return false;
                }

                // Add back to queue
                if (!this.queues.ContainsKey(inProcess.Item1))
                {
                    this.queues.Add(inProcess.Item1, new Queue<byte[]>());
                }

                this.queues[inProcess.Item1].Enqueue(inProcess.Item3[0]);

                // Remove from invisible queue
                inProcess.Item3.RemoveAt(0);
                if (inProcess.Item3.Count == 0)
                {
                    this.inProcessMessages.Remove(inProcess.Item2);
                }

                return true;
            }
        }

        /// <summary>
        /// Abandon all messages still being processed. This is recommended to call e.g. when forcing a worker to shutting.
        /// </summary>
        /// <returns>
        /// The number of original messages actually deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public int AbandonAll()
        {
            var count = 0;
            lock (this.sync)
            {
                while (this.inProcessMessages.Count > 0)
                {
                    count += this.AbandonRange(this.inProcessMessages.Keys.ToList());
                }
            }

            return count;
        }

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
        /// </remarks>
        public int AbandonRange<T>(
            IEnumerable<T> messages, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            lock (this.sync)
            {
                return messages.Count(m => this.Abandon(m, timeToLive, delay));
            }
        }

        /// <summary>
        /// Clear all the messages from the specified queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void Clear(string queueName)
        {
            lock (this.sync)
            {
                this.queues[queueName].Clear();

                var toDelete = this.inProcessMessages.Where(pair => pair.Value.Item1 == queueName).ToList();
                foreach (var pair in toDelete)
                {
                    this.inProcessMessages.Remove(pair.Key);
                }
            }
        }

        /// <summary>
        /// Deletes a message being processed from the queue.
        /// </summary>
        /// <typeparam name="T">
        /// The message type.
        /// </typeparam>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <returns>
        /// <c>True</c> if the message has been deleted. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool Delete<T>(T message)
        {
            lock (this.sync)
            {
                Tu inProcess;
                if (!this.inProcessMessages.TryGetValue(message, out inProcess))
                {
                    return false;
                }

                inProcess.Item3.RemoveAt(0);
                if (inProcess.Item3.Count == 0)
                {
                    this.inProcessMessages.Remove(inProcess.Item2);
                }

                return true;
            }
        }

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
        public void DeletePersisted(string storeName, string key, IDataSerializer serializer = null)
        {
            lock (this.sync)
            {
                this.persistedMessages.RemoveWhere(x => x.Item1 == storeName && x.Item2 == key);
            }
        }

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
        public bool DeleteQueue(string queueName)
        {
            lock (this.sync)
            {
                if (!this.queues.ContainsKey(queueName))
                {
                    return false;
                }

                this.queues.Remove(queueName);

                var toDelete = this.inProcessMessages.Where(pair => pair.Value.Item1 == queueName).ToList();
                foreach (var pair in toDelete)
                {
                    this.inProcessMessages.Remove(pair.Key);
                }

                return true;
            }
        }

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
        /// </remarks>
        public int DeleteRange<T>(IEnumerable<T> messages)
        {
            lock (this.sync)
            {
                return messages.Where(this.Delete).Count();
            }
        }

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
        public IEnumerable<T> Get<T>(
            string queueName, 
            int count, 
            TimeSpan visibilityTimeout, 
            int maxProcessingTrials, 
            IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? this.DefaultSerializer;
            lock (this.sync)
            {
                var items = new List<T>(count);
                for (var i = 0; i < count; i++)
                {
                    if (this.queues.ContainsKey(queueName) && this.queues[queueName].Any())
                    {
                        var messageBytes = this.queues[queueName].Dequeue();
                        object message;
                        using (var stream = new MemoryStream(messageBytes))
                        {
                            message = dataSerializer.Deserialize(stream, typeof(T));
                        }

                        Tu inProcess;
                        if (!this.inProcessMessages.TryGetValue(message, out inProcess))
                        {
                            inProcess = new Tu(queueName, message, new List<byte[]>());
                            this.inProcessMessages.Add(message, inProcess);
                        }

                        inProcess.Item3.Add(messageBytes);
                        items.Add((T)message);
                    }
                }

                return items;
            }
        }

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
        public int GetApproximateCount(string queueName)
        {
            lock (this.sync)
            {
                Queue<byte[]> queue;
                return this.queues.TryGetValue(queueName, out queue) ? queue.Count : 0;
            }
        }

        /// <summary>
        /// Gets the approximate age of the top message of this queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// A time span.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<TimeSpan> GetApproximateLatency(string queueName)
        {
            return Maybe<TimeSpan>.Empty;
        }

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
        /// The persisted message.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<PersistedMessage> GetPersisted(string storeName, string key, IDataSerializer serializer = null)
        {
            var intermediateDataSerializer = (serializer ?? this.DefaultSerializer) as IIntermediateDataSerializer;
            var xmlProvider = intermediateDataSerializer != null
                                  ? new Maybe<IIntermediateDataSerializer>(intermediateDataSerializer)
                                  : Maybe<IIntermediateDataSerializer>.Empty;

            lock (this.sync)
            {
                var tuple = this.persistedMessages.FirstOrDefault(x => x.Item1 == storeName && x.Item2 == key);
                if (null != tuple)
                {
                    return new PersistedMessage
                        {
                            QueueName = tuple.Item3, 
                            StoreName = tuple.Item1, 
                            Key = tuple.Item2, 
                            IsDataAvailable = true, 
                            DataXml = xmlProvider.Convert(s => s.UnpackXml(new MemoryStream(tuple.Item4)))
                        };
                }

                return Maybe<PersistedMessage>.Empty;
            }
        }

        /// <summary>
        /// Keep the message alive for another period.
        /// </summary>
        /// <typeparam name="T">
        /// The type of message.
        /// </typeparam>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <returns>
        /// The new visibility timeout 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TimeSpan KeepAlive<T>(T message) where T : class
        {
            return TimeSpan.FromMinutes(5);
        }

        /// <summary>
        /// Gets the list of queues whose name start with the specified prefix.
        /// </summary>
        /// <param name="prefix">
        /// If <c>null</c> or empty, returns all queues. 
        /// </param>
        /// <returns>
        /// The queue names.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> List(string prefix)
        {
            return this.queues.Keys.Where(e => e.StartsWith(prefix));
        }

        /// <summary>
        /// Enumerate the keys of all persisted messages of the provided store.
        /// </summary>
        /// <param name="storeName">
        /// Name of the message persistence store. 
        /// </param>
        /// <returns>
        /// The queue names.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public IEnumerable<string> ListPersisted(string storeName)
        {
            lock (this.sync)
            {
                return this.persistedMessages.Where(x => x.Item1 == storeName).Select(x => x.Item2).ToArray();
            }
        }

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
        public void Persist<T>(T message, string storeName, string reason)
        {
            lock (this.sync)
            {
                Tu inProcess;
                if (!this.inProcessMessages.TryGetValue(message, out inProcess))
                {
                    return;
                }

                // persist
                var key = Guid.NewGuid().ToString("N");
                this.persistedMessages.Add(Tuple.Create(storeName, key, inProcess.Item1, inProcess.Item3[0]));

                // Remove from invisible queue
                inProcess.Item3.RemoveAt(0);
                if (inProcess.Item3.Count == 0)
                {
                    this.inProcessMessages.Remove(inProcess.Item2);
                }
            }
        }

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
        public void PersistRange<T>(IEnumerable<T> messages, string storeName, string reason)
        {
            lock (this.sync)
            {
                foreach (var message in messages)
                {
                    this.Persist(message, storeName, reason);
                }
            }
        }

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
        /// </remarks>
        public void Put<T>(
            string queueName, 
            T message, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? this.DefaultSerializer;
            lock (this.sync)
            {
                byte[] messageBytes;
                using (var stream = new MemoryStream())
                {
                    dataSerializer.Serialize(message, stream, typeof(T));
                    messageBytes = stream.ToArray();
                }

                if (!this.queues.ContainsKey(queueName))
                {
                    this.queues.Add(queueName, new Queue<byte[]>());
                }

                this.queues[queueName].Enqueue(messageBytes);
            }
        }

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
        /// </remarks>
        public void PutRange<T>(
            string queueName, 
            IEnumerable<T> messages, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? this.DefaultSerializer;
            lock (this.sync)
            {
                foreach (var message in messages)
                {
                    this.Put(queueName, message, timeToLive, delay, dataSerializer);
                }
            }
        }

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
        /// </remarks>
        public void PutRangeParallel<T>(
            string queueName, 
            IEnumerable<T> messages, 
            TimeSpan timeToLive = default(TimeSpan), 
            TimeSpan delay = default(TimeSpan), 
            IDataSerializer serializer = null)
        {
            this.PutRange(queueName, messages, timeToLive, delay, serializer);
        }

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
        public void RestorePersisted(
            string storeName, string key, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            lock (this.sync)
            {
                var item = this.persistedMessages.First(x => x.Item1 == storeName && x.Item2 == key);
                this.persistedMessages.Remove(item);

                if (!this.queues.ContainsKey(item.Item3))
                {
                    this.queues.Add(item.Item3, new Queue<byte[]>());
                }

                this.queues[item.Item3].Enqueue(item.Item4);
            }
        }

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
        /// </remarks>
        public bool ResumeLater<T>(
            T message, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            // same as abandon as the InMemory provider applies no poison detection
            return this.Abandon(message);
        }

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
        /// </remarks>
        public int ResumeLaterRange<T>(
            IEnumerable<T> messages, TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            // same as abandon as the InMemory provider applies no poison detection
            return this.AbandonRange(messages);
        }

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
        public int ReviveMessages(TimeSpan timeToLive = default(TimeSpan), TimeSpan delay = default(TimeSpan))
        {
            return 0;
        }

        #endregion
    }
}