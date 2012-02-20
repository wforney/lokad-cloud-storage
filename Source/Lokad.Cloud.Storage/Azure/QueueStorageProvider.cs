#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Instrumentation;
    using Lokad.Cloud.Storage.Instrumentation.Events;
    using Lokad.Cloud.Storage.Queues;
    using Lokad.Cloud.Storage.Shared.Monads;

    using Microsoft.WindowsAzure.StorageClient;

    /// <summary>
    /// Provides access to the Queue Storage (plus the Blob Storage when messages are overflowing).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Overflowing messages are stored in blob storage and normally deleted as with
    ///     their originating correspondence in queue storage.
    /// </para>
    /// <para>
    /// All the methods of
    ///                                                                 <see cref="QueueStorageProvider"/>
    ///                                                                 are thread-safe.
    /// </para>
    /// </remarks>
    public class QueueStorageProvider : IQueueStorageProvider
    {
        #region Constants and Fields

        /// <summary>
        /// The overflowing messages container name.
        /// </summary>
        internal const string OverflowingMessagesContainerName = "lokad-cloud-overflowing-messages";

        /// <summary>
        /// The poisoned message persistence store name.
        /// </summary>
        internal const string PoisonedMessagePersistenceStoreName = "failing-messages";

        /// <summary>
        /// The resilient leases container name.
        /// </summary>
        internal const string ResilientLeasesContainerName = "lokad-cloud-resilient-leases";

        /// <summary>
        /// The resilient messages container name.
        /// </summary>
        internal const string ResilientMessagesContainerName = "lokad-cloud-resilient-messages";

        /// <summary>
        /// The keep alive visibility timeout.
        /// </summary>
        private static readonly TimeSpan KeepAliveVisibilityTimeout = TimeSpan.FromSeconds(60);

        /// <summary>
        /// The _blob storage.
        /// </summary>
        private readonly IBlobStorageProvider blobStorage;

        /// <summary>
        /// The _default serializer.
        /// </summary>
        private readonly IDataSerializer defaultSerializer;

        // messages currently being processed (boolean property indicates if the message is overflowing)
        /// <summary>
        ///   Mapping object --&gt; Queue Message Id. Use to delete messages afterward.
        /// </summary>
        private readonly Dictionary<object, InProcessMessage> inProcessMessages;

        /// <summary>
        /// The _observer.
        /// </summary>
        private readonly IStorageObserver observer;

        /// <summary>
        /// The _policies.
        /// </summary>
        private readonly RetryPolicies policies;

        /// <summary>
        /// The _queue storage.
        /// </summary>
        private readonly CloudQueueClient queueStorage;

        /// <summary>
        ///   Root used to synchronize accesses to <c>_inprocess</c> . Caution: do not hold the lock while performing operations on the cloud storage.
        /// </summary>
        private readonly object sync = new object();

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorageProvider"/> class. 
        /// IoC constructor.
        /// </summary>
        /// <param name="queueStorage">
        /// The queue storage. 
        /// </param>
        /// <param name="blobStorage">
        /// The BLOB storage. 
        /// </param>
        /// <param name="defaultSerializer">
        /// The default serializer. 
        /// </param>
        /// <param name="observer">
        /// The observer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public QueueStorageProvider(
            CloudQueueClient queueStorage, 
            IBlobStorageProvider blobStorage, 
            IDataSerializer defaultSerializer, 
            IStorageObserver observer = null)
        {
            this.policies = new RetryPolicies(observer);
            this.queueStorage = queueStorage;
            this.blobStorage = blobStorage;
            this.defaultSerializer = defaultSerializer;
            this.observer = observer;

            this.inProcessMessages = new Dictionary<object, InProcessMessage>(20, new IdentityComparer());
        }

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
            var stopwatch = new Stopwatch();

            // 1. GET RAW MESSAGE & QUEUE, OR SKIP IF NOT AVAILABLE/ALREADY DELETED
            CloudQueueMessage oldRawMessage;
            string queueName;
            int dequeueCount;
            byte[] data;
            IDataSerializer dataSerializer;
            string keepAliveBlobName;
            string keepAliveBlobLease;

            lock (this.sync)
            {
                // ignoring message if already deleted
                InProcessMessage inProcMsg;
                if (!this.inProcessMessages.TryGetValue(message, out inProcMsg)
                    || (IdentityComparer.CanDifferentiateInstances(typeof(T)) && inProcMsg.CommitStarted))
                {
                    return false;
                }

                queueName = inProcMsg.QueueName;
                dequeueCount = inProcMsg.DequeueCount;
                oldRawMessage = inProcMsg.RawMessages[0];
                data = inProcMsg.Data;
                dataSerializer = inProcMsg.Serializer;
                keepAliveBlobName = inProcMsg.KeepAliveBlobName;
                keepAliveBlobLease = inProcMsg.KeepAliveBlobLease;

                inProcMsg.CommitStarted = true;
            }

            var queue = this.queueStorage.GetQueueReference(queueName);

            // 2. CLONE THE MESSAGE AND PUT IT TO THE QUEUE
            // we always use an envelope here since the dequeue count
            // is always >0, which we should continue to track in order
            // to make poison detection possible at all.
            var envelope = new MessageEnvelope { DequeueCount = dequeueCount, RawMessage = data };

            CloudQueueMessage newRawMessage = null;
            using (var stream = new MemoryStream())
            {
                dataSerializer.Serialize(envelope, stream, typeof(MessageEnvelope));
                if (stream.Length < (CloudQueueMessage.MaxMessageSize - 1) / 4 * 3)
                {
                    try
                    {
                        newRawMessage = new CloudQueueMessage(stream.ToArray());
                    }
                    catch (ArgumentException)
                    {
                    }
                }

                if (newRawMessage == null)
                {
                    envelope.RawMessage = this.PutOverflowingMessageAndWrap(queueName, message, dataSerializer);
                    using (var wrappedStream = new MemoryStream())
                    {
                        dataSerializer.Serialize(envelope, wrappedStream, typeof(MessageEnvelope));
                        newRawMessage = new CloudQueueMessage(wrappedStream.ToArray());
                    }
                }
            }

            this.PutRawMessage(newRawMessage, queue, timeToLive, delay);

            // 3. DELETE THE OLD MESSAGE FROM THE QUEUE
            bool deleted;
            if (keepAliveBlobName != null && keepAliveBlobLease != null)
            {
                // CASE: in resilient mode
                // => release locks, delete blobs
                deleted = this.DeleteKeepAliveMessage(keepAliveBlobName, keepAliveBlobLease);
            }
            else
            {
                // CASE: normal mode (or keep alive in progress)
                // => just delete the message
                deleted = this.DeleteRawMessage(oldRawMessage, queue);
            }

            // 4. REMOVE THE RAW MESSAGE
            this.CheckInMessage(message);

            if (deleted)
            {
                this.NotifySucceeded(StorageOperationType.QueueAbandon, stopwatch);
                return true;
            }

            return false;
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
            while (true)
            {
                List<object> messages;
                lock (this.sync)
                {
                    messages = this.inProcessMessages.Keys.ToList();
                }

                if (messages.Count == 0)
                {
                    return count;
                }

                count += this.AbandonRange(messages);
            }
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
            return messages.Count(m => this.Abandon(m, timeToLive, delay));
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
            try
            {
                // caution: call 'DeleteOverflowingMessages' first (BASE).
                this.DeleteOverflowingMessages(queueName);
                var queue = this.queueStorage.GetQueueReference(queueName);
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, queue.Clear);
            }
            catch (StorageClientException ex)
            {
                // if the queue does not exist do nothing
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return;
                }

                throw;
            }
        }

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
        /// </remarks>
        public bool Delete<T>(T message)
        {
            var stopwatch = new Stopwatch();

            // 1. GET RAW MESSAGE & QUEUE, OR SKIP IF NOT AVAILABLE/ALREADY DELETED
            CloudQueueMessage rawMessage;
            string queueName;
            bool isOverflowing;
            byte[] data;
            IDataSerializer dataSerializer;
            string keepAliveBlobName;
            string keepAliveBlobLease;

            lock (this.sync)
            {
                // ignoring message if already deleted
                InProcessMessage inProcMsg;
                if (!this.inProcessMessages.TryGetValue(message, out inProcMsg)
                    || (IdentityComparer.CanDifferentiateInstances(typeof(T)) && inProcMsg.CommitStarted))
                {
                    return false;
                }

                rawMessage = inProcMsg.RawMessages[0];
                isOverflowing = inProcMsg.IsOverflowing;
                queueName = inProcMsg.QueueName;
                data = inProcMsg.Data;
                dataSerializer = inProcMsg.Serializer;
                keepAliveBlobName = inProcMsg.KeepAliveBlobName;
                keepAliveBlobLease = inProcMsg.KeepAliveBlobLease;

                inProcMsg.CommitStarted = true;
            }

            // 2. DELETING THE OVERFLOW BLOB, IF WRAPPED
            if (isOverflowing)
            {
                var messageWrapper = dataSerializer.TryDeserializeAs<MessageWrapper>(data);
                if (messageWrapper.IsSuccess)
                {
                    this.blobStorage.DeleteBlobIfExist(
                        messageWrapper.Value.ContainerName, messageWrapper.Value.BlobName);
                }
            }

            // 3. DELETE THE MESSAGE FROM THE QUEUE
            bool deleted;
            if (keepAliveBlobName != null && keepAliveBlobLease != null)
            {
                // CASE: in resilient mode
                // => release locks, delete blobs
                deleted = this.DeleteKeepAliveMessage(keepAliveBlobName, keepAliveBlobLease);
            }
            else
            {
                // CASE: normal mode (or keep alive in progress)
                // => just delete the message
                var queue = this.queueStorage.GetQueueReference(queueName);
                deleted = this.DeleteRawMessage(rawMessage, queue);
            }

            // 4. REMOVE THE RAW MESSAGE
            this.CheckInMessage(message);

            if (deleted)
            {
                this.NotifySucceeded(StorageOperationType.QueueDelete, stopwatch);
                return true;
            }

            return false;
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
            var dataSerializer = serializer ?? this.defaultSerializer;

            // 1. GET PERSISTED MESSAGE BLOB
            var blobReference = new PersistedMessageBlobName(storeName, key);
            var blob = this.blobStorage.GetBlob(blobReference);
            if (!blob.HasValue)
            {
                return;
            }

            var persistedMessage = blob.Value;

            // 2. IF WRAPPED, UNWRAP AND DELETE BLOB
            var messageWrapper = dataSerializer.TryDeserializeAs<MessageWrapper>(persistedMessage.Data);
            if (messageWrapper.IsSuccess)
            {
                this.blobStorage.DeleteBlobIfExist(messageWrapper.Value.ContainerName, messageWrapper.Value.BlobName);
            }

            // 3. DELETE PERSISTED MESSAGE
            this.blobStorage.DeleteBlobIfExist(blobReference);
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
        /// This implementation takes care of deleting overflowing blobs as well.
        /// </remarks>
        public bool DeleteQueue(string queueName)
        {
            try
            {
                // Caution: call to 'DeleteOverflowingMessages' comes first (BASE).
                this.DeleteOverflowingMessages(queueName);
                var queue = this.queueStorage.GetQueueReference(queueName);
                Retry.Do(this.policies.TransientServerErrorBackOff, CancellationToken.None, queue.Delete);
                return true;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return false;
                }

                throw;
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
            return messages.Count(this.Delete);
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
            var stopwatch = Stopwatch.StartNew();
            var dataSerializer = serializer ?? this.defaultSerializer;

            var queue = this.queueStorage.GetQueueReference(queueName);

            // 1. GET RAW MESSAGES
            IEnumerable<CloudQueueMessage> rawMessages;

            try
            {
                rawMessages = Retry.Get(
                    this.policies.TransientServerErrorBackOff, 
                    CancellationToken.None, 
                    () => queue.GetMessages(count, visibilityTimeout));
            }
            catch (StorageClientException ex)
            {
                // if the queue does not exist return an empty collection.
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return new T[0];
                }

                throw;
            }

            // 2. SKIP EMPTY QUEUE
            if (null == rawMessages)
            {
                this.NotifySucceeded(StorageOperationType.QueueGet, stopwatch);
                return new T[0];
            }

            // 3. DESERIALIZE MESSAGE OR MESSAGE WRAPPER, CHECK-OUT
            var messages = new List<T>(count);
            var wrappedMessages = new List<MessageWrapper>();

            foreach (var rawMessage in rawMessages)
            {
                // 3.1. DESERIALIZE MESSAGE, CHECK-OUT, COLLECT WRAPPED MESSAGES TO BE UNWRAPPED LATER
                var data = rawMessage.AsBytes;
                var stream = new MemoryStream(data);
                try
                {
                    var dequeueCount = rawMessage.DequeueCount;

                    // 3.1.1 UNPACK ENVELOPE IF PACKED, UPDATE POISONING INDICATOR
                    var messageAsEnvelope = dataSerializer.TryDeserializeAs<MessageEnvelope>(stream);
                    if (messageAsEnvelope.IsSuccess)
                    {
                        stream.Dispose();
                        dequeueCount += messageAsEnvelope.Value.DequeueCount;
                        data = messageAsEnvelope.Value.RawMessage;
                        stream = new MemoryStream(data);
                    }

                    // 3.1.2 PERSIST POISONED MESSAGE, SKIP
                    if (dequeueCount > maxProcessingTrials)
                    {
                        // we want to persist the unpacked message (no envelope) but still need to drop
                        // the original message, that's why we pass the original rawMessage but the unpacked data
                        this.PersistRawMessage(
                            rawMessage, 
                            data, 
                            queueName, 
                            PoisonedMessagePersistenceStoreName, 
                            string.Format(
                                "Message was dequeued {0} times but failed processing each time.", dequeueCount - 1));

                        if (this.observer != null)
                        {
                            this.observer.Notify(
                                new MessageProcessingFailedQuarantinedEvent(
                                    queueName, PoisonedMessagePersistenceStoreName, typeof(T), data));
                        }

                        continue;
                    }

                    // 3.1.3 DESERIALIZE MESSAGE IF POSSIBLE
                    var messageAsT = dataSerializer.TryDeserializeAs<T>(stream);
                    if (messageAsT.IsSuccess)
                    {
                        messages.Add(messageAsT.Value);
                        this.CheckOutMessage(
                            messageAsT.Value, rawMessage, data, queueName, false, dequeueCount, dataSerializer);

                        continue;
                    }

                    // 3.1.4 DESERIALIZE WRAPPER IF POSSIBLE
                    var messageAsWrapper = dataSerializer.TryDeserializeAs<MessageWrapper>(stream);
                    if (messageAsWrapper.IsSuccess)
                    {
                        // we don't retrieve messages while holding the lock
                        wrappedMessages.Add(messageAsWrapper.Value);
                        this.CheckOutMessage(
                            messageAsWrapper.Value, rawMessage, data, queueName, true, dequeueCount, dataSerializer);

                        continue;
                    }

                    // 3.1.5 PERSIST FAILED MESSAGE, SKIP

                    // we want to persist the unpacked message (no envelope) but still need to drop
                    // the original message, that's why we pass the original rawMessage but the unpacked data
                    this.PersistRawMessage(
                        rawMessage, 
                        data, 
                        queueName, 
                        PoisonedMessagePersistenceStoreName, 
                        string.Format(
                            "Message failed to deserialize:\r\nAs {0}:\r\n{1}\r\n\r\nAs MessageEnvelope:\r\n{2}\r\n\r\nAs MessageWrapper:\r\n{3}", 
                            typeof(T).FullName, 
                            messageAsT.Error, 
                            messageAsEnvelope.IsSuccess ? "unwrapped" : messageAsEnvelope.Error.ToString(), 
                            messageAsWrapper.Error));

                    if (this.observer != null)
                    {
                        var exceptions = new List<Exception> { messageAsT.Error, messageAsWrapper.Error };
                        if (!messageAsEnvelope.IsSuccess)
                        {
                            exceptions.Add(messageAsEnvelope.Error);
                        }

                        this.observer.Notify(
                            new MessageDeserializationFailedQuarantinedEvent(
                                new AggregateException(exceptions), 
                                queueName, 
                                PoisonedMessagePersistenceStoreName, 
                                typeof(T), 
                                data));
                    }
                }
                finally
                {
                    stream.Dispose();
                }
            }

            // 4. UNWRAP WRAPPED MESSAGES
            var unwrapStopwatch = new Stopwatch();
            foreach (var mw in wrappedMessages)
            {
                unwrapStopwatch.Restart();

                string ignored;
                var blobContent = this.blobStorage.GetBlob(mw.ContainerName, mw.BlobName, typeof(T), out ignored);

                // blob may not exists in (rare) case of failure just before queue deletion
                // but after container deletion (or also timeout deletion).
                if (!blobContent.HasValue)
                {
                    CloudQueueMessage rawMessage;
                    lock (this.sync)
                    {
                        rawMessage = this.inProcessMessages[mw].RawMessages[0];
                        this.CheckInMessage(mw);
                    }

                    this.DeleteRawMessage(rawMessage, queue);

                    // skipping the message if it can't be unwrapped
                    continue;
                }

                var innerMessage = (T)blobContent.Value;

                // substitution: message wrapper replaced by actual item in '_inprocess' list
                this.CheckOutRelink(mw, innerMessage);

                messages.Add(innerMessage);
                this.NotifySucceeded(StorageOperationType.QueueUnwrap, unwrapStopwatch);
            }

            this.NotifySucceeded(StorageOperationType.QueueGet, stopwatch);

            // 5. RETURN LIST OF MESSAGES
            return messages;
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
            try
            {
                var queue = this.queueStorage.GetQueueReference(queueName);
                return Retry.Get(
                    this.policies.TransientServerErrorBackOff, 
                    CancellationToken.None, 
                    queue.RetrieveApproximateMessageCount);
            }
            catch (StorageClientException ex)
            {
                // if the queue does not exist, return 0 (no queue)
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return 0;
                }

                throw;
            }
        }

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
        public Maybe<TimeSpan> GetApproximateLatency(string queueName)
        {
            var queue = this.queueStorage.GetQueueReference(queueName);
            CloudQueueMessage rawMessage;

            try
            {
                rawMessage = Retry.Get(
                    this.policies.TransientServerErrorBackOff, CancellationToken.None, queue.PeekMessage);
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return Maybe<TimeSpan>.Empty;
                }

                throw;
            }

            if (rawMessage == null || !rawMessage.InsertionTime.HasValue)
            {
                return Maybe<TimeSpan>.Empty;
            }

            var latency = DateTimeOffset.UtcNow - rawMessage.InsertionTime.Value;

            // don't return negative values when clocks are slightly out of sync 
            return latency > TimeSpan.Zero ? latency : TimeSpan.Zero;
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
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<PersistedMessage> GetPersisted(string storeName, string key, IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? this.defaultSerializer;

            // 1. GET PERSISTED MESSAGE BLOB
            var blobReference = new PersistedMessageBlobName(storeName, key);
            var blob = this.blobStorage.GetBlob(blobReference);
            if (!blob.HasValue)
            {
                return Maybe<PersistedMessage>.Empty;
            }

            var persistedMessage = blob.Value;
            var data = persistedMessage.Data;
            var dataXml = Maybe<XElement>.Empty;

            // 2. IF WRAPPED, UNWRAP; UNPACK XML IF SUPPORTED
            bool dataForRestorationAvailable;
            var messageWrapper = dataSerializer.TryDeserializeAs<MessageWrapper>(data);
            if (messageWrapper.IsSuccess)
            {
                string ignored;
                dataXml = this.blobStorage.GetBlobXml(
                    messageWrapper.Value.ContainerName, messageWrapper.Value.BlobName, out ignored);

                // We consider data to be available only if we can access its blob's data
                // Simplification: we assume that if we can get the data as xml, then we can also get its binary data
                dataForRestorationAvailable = dataXml.HasValue;
            }
            else
            {
                var intermediateSerializer = dataSerializer as IIntermediateDataSerializer;
                if (intermediateSerializer != null)
                {
                    using (var stream = new MemoryStream(data))
                    {
                        var unpacked = intermediateSerializer.TryUnpackXml(stream);
                        dataXml = unpacked.IsSuccess ? unpacked.Value : Maybe<XElement>.Empty;
                    }
                }

                // The message is not wrapped (or unwrapping it failed).
                // No matter whether we can get the xml, we do have access to the binary data
                dataForRestorationAvailable = true;
            }

            // 3. RETURN
            return new PersistedMessage
                {
                    QueueName = persistedMessage.QueueName, 
                    StoreName = storeName, 
                    Key = key, 
                    InsertionTime = persistedMessage.InsertionTime, 
                    PersistenceTime = persistedMessage.PersistenceTime, 
                    DequeueCount = persistedMessage.DequeueCount, 
                    Reason = persistedMessage.Reason, 
                    DataXml = dataXml, 
                    IsDataAvailable = dataForRestorationAvailable, 
                };
        }

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
        public TimeSpan KeepAlive<T>(T message) where T : class
        {
            if (!IdentityComparer.CanDifferentiateInstances(typeof(T)))
            {
                throw new NotSupportedException("KeepAlive supports neither strings nor value types");
            }

            CloudQueueMessage rawMessage;
            string queueName;
            byte[] data;
            string blobName;
            string blobLease;

            lock (this.sync)
            {
                InProcessMessage inProcMsg;
                if (!this.inProcessMessages.TryGetValue(message, out inProcMsg) || inProcMsg.CommitStarted)
                {
                    // CASE: the message has already been handled => we ignore the request
                    return TimeSpan.Zero;
                }

                rawMessage = inProcMsg.RawMessages[0];
                queueName = inProcMsg.QueueName;
                data = inProcMsg.Data;
                blobName = inProcMsg.KeepAliveBlobName;
                blobLease = inProcMsg.KeepAliveBlobLease;

                if (blobName == null)
                {
                    // CASE: this is the first call to KeepAlive.
                    // => choose a name and set the initial invisibility time; continue
                    blobName = inProcMsg.KeepAliveBlobName = Guid.NewGuid().ToString("N");
                    inProcMsg.KeepAliveTimeout = DateTimeOffset.UtcNow + KeepAliveVisibilityTimeout;
                }
                else if (blobLease == null)
                {
                    // CASE: the message is already being initialized. This can happen
                    // e.g. on two calls to KeepAlive form different threads (race).
                    // => do nothing, but only return the remaining invisibility time
                    return inProcMsg.KeepAliveTimeout - DateTimeOffset.UtcNow;
                }

                // ELSE CASE: this is a successive call; continue
            }

            if (blobLease != null)
            {
                // CASE: this is a successive call, the message is already resilient
                // => just renew the lease
                var messageAlreadyHandled = false;
                Retry.DoUntilTrue(
                    this.policies.OptimisticConcurrency, 
                    CancellationToken.None, 
                    () =>
                        {
                            var result = this.blobStorage.TryRenewLease(
                                ResilientLeasesContainerName, blobName, blobLease);
                            if (result.IsSuccess)
                            {
                                // CASE: success
                                return true;
                            }

                            if (result.Error == "NotFound")
                            {
                                // CASE: we managed to loose our lease file, meaning that we must have lost our lease
                                // (maybe because we didn't renew in time) and the message was handled in the meantime.
                                // => do nothing
                                messageAlreadyHandled = true;
                                return true;
                            }

                            if (result.Error == "Conflict")
                            {
                                // CASE: we managed to loose our lease and someone acquired it in the meantime
                                // => try to re-aquire a new lease
                                var newLease = this.blobStorage.TryAcquireLease(ResilientLeasesContainerName, blobName);
                                if (newLease.IsSuccess)
                                {
                                    // CASE: we managed to re-acquire the lost lease.
                                    // However, if the message blob is no longer present then the message was already handled and we need to retreat

                                    if (this.blobStorage.GetBlobEtag(ResilientMessagesContainerName, blobName) == null)
                                    {
                                        Retry.DoUntilTrue(
                                            this.policies.OptimisticConcurrency, 
                                            CancellationToken.None, 
                                            () =>
                                                {
                                                    var retreatResult =
                                                        this.blobStorage.TryReleaseLease(
                                                            ResilientLeasesContainerName, blobName, newLease.Value);
                                                    return retreatResult.IsSuccess || result.Error == "NotFound";
                                                });
                                        messageAlreadyHandled = true;
                                        return true;
                                    }

                                    blobLease = newLease.Value;
                                    return true;
                                }

                                if (newLease.Error == "NotFound")
                                {
                                    // CASE: we managed to loose our lease file, meaning that we must have lost our lease
                                    // (maybe because we didn't renew in time) and the message was handled in the meantime.
                                    // => do nothing
                                    messageAlreadyHandled = true;
                                    return true;
                                }

                                // still conflict or transient error, retry
                                return false;
                            }

                            return false;
                        });

                if (messageAlreadyHandled)
                {
                    return TimeSpan.Zero;
                }

                lock (this.sync)
                {
                    InProcessMessage inProcMsg;
                    if (!this.inProcessMessages.TryGetValue(message, out inProcMsg) || inProcMsg.CommitStarted)
                    {
                        // CASE: Renew worked, but in the meantime the message has already be handled
                        // => do nothing
                        return TimeSpan.Zero;
                    }

                    // CASE: renew succeeded, or we managed to acquire a new lease
                    inProcMsg.KeepAliveTimeout = DateTimeOffset.UtcNow - KeepAliveVisibilityTimeout;
                    inProcMsg.KeepAliveBlobLease = blobLease;
                    return KeepAliveVisibilityTimeout;
                }
            }

            // CASE: this is the first call to KeepAlive

            // 1. CREATE LEASE OBJECT
            this.blobStorage.PutBlob(
                ResilientLeasesContainerName, 
                blobName, 
                new ResilientLeaseData { QueueName = queueName, BlobName = blobName });

            // 2. TAKE LEASE ON LEASE OBJECT

            Retry.DoUntilTrue(
                this.policies.OptimisticConcurrency, 
                CancellationToken.None, 
                () =>
                    {
                        var lease = this.blobStorage.TryAcquireLease(ResilientLeasesContainerName, blobName);
                        if (lease.IsSuccess)
                        {
                            blobLease = lease.Value;
                            return true;
                        }

                        if (lease.Error == "NotFound")
                        {
                            // CASE: lease blob has been deleted before we could acquire the lease
                            // => recreate the blob, then retry
                            this.blobStorage.PutBlob(
                                ResilientLeasesContainerName, 
                                blobName, 
                                new ResilientLeaseData { QueueName = queueName, BlobName = blobName });
                            return false;
                        }

                        // CASE: conflict (e.g. because ReviveMessages is running), or transient error
                        // => retry
                        return false;
                    });

            // 3. PUT MESSAGE TO BLOB
            this.blobStorage.PutBlob(
                ResilientMessagesContainerName, 
                blobName, 
                new ResilientMessageData { QueueName = queueName, Data = data });

            // 4. UPDATE IN-PROCESS-MESSAGE
            var rollback = false;
            lock (this.sync)
            {
                InProcessMessage inProcMsg;
                if (!this.inProcessMessages.TryGetValue(message, out inProcMsg) || inProcMsg.CommitStarted)
                {
                    rollback = true;
                }
                else
                {
                    inProcMsg.KeepAliveBlobLease = blobLease;
                }
            }

            // 5. ROLLBACK IF MESSAGE HAS BEEN HANDLED IN THE MEANTIME

            if (rollback)
            {
                // CASE: The message has been handled in the meantime (so this call should be ignored)
                // => Drop all the blobs we created and exit
                this.blobStorage.DeleteBlobIfExist(ResilientMessagesContainerName, blobName);

                Retry.DoUntilTrue(
                    this.policies.OptimisticConcurrency, 
                    CancellationToken.None, 
                    () =>
                        {
                            var result = this.blobStorage.TryReleaseLease(
                                ResilientLeasesContainerName, blobName, blobLease);
                            if (result.IsSuccess)
                            {
                                this.blobStorage.DeleteBlobIfExist(ResilientLeasesContainerName, blobName);
                                return true;
                            }

                            return result.Error == "NotFound";
                        });
                return TimeSpan.Zero;
            }

            // 6. DELETE MESSAGE FROM THE QUEUE
            var queue = this.queueStorage.GetQueueReference(queueName);
            this.DeleteRawMessage(rawMessage, queue);

            return KeepAliveVisibilityTimeout;
        }

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
        public IEnumerable<string> List(string prefix)
        {
            return this.queueStorage.ListQueues(prefix).Select(queue => queue.Name);
        }

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
        public IEnumerable<string> ListPersisted(string storeName)
        {
            var blobPrefix = PersistedMessageBlobName.GetPrefix(storeName);
            return this.blobStorage.ListBlobNames(blobPrefix).Select(blobReference => blobReference.Key);
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
            // 1. GET MESSAGE FROM CHECK-OUT, SKIP IF NOT AVAILABLE/ALREADY DELETED
            CloudQueueMessage rawMessage;
            string queueName;
            byte[] data;

            lock (this.sync)
            {
                // ignoring message if already deleted
                InProcessMessage inProcessMessage;
                if (!this.inProcessMessages.TryGetValue(message, out inProcessMessage))
                {
                    return;
                }

                queueName = inProcessMessage.QueueName;
                rawMessage = inProcessMessage.RawMessages[0];
                data = inProcessMessage.Data;
            }

            // 2. PERSIST MESSAGE AND DELETE FROM QUEUE
            this.PersistRawMessage(rawMessage, data, queueName, storeName, reason);

            // 3. REMOVE MESSAGE FROM CHECK-OUT
            this.CheckInMessage(message);
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
            foreach (var message in messages)
            {
                this.Persist(message, storeName, reason);
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
            this.PutRange(queueName, new[] { message }, timeToLive, delay, serializer);
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
            var dataSerializer = serializer ?? this.defaultSerializer;
            var queue = this.queueStorage.GetQueueReference(queueName);
            var stopwatch = new Stopwatch();

            foreach (var message in messages)
            {
                stopwatch.Restart();

                var queueMessage = this.SerializeCloudQueueMessage(queueName, message, dataSerializer);

                this.PutRawMessage(queueMessage, queue, timeToLive, delay);

                this.NotifySucceeded(StorageOperationType.QueuePut, stopwatch);
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
            var dataSerializer = serializer ?? this.defaultSerializer;
            var queue = this.queueStorage.GetQueueReference(queueName);
            var stopwatch = new Stopwatch();

            var tasks = new List<Task>();

            foreach (var message in messages)
            {
                stopwatch.Restart();

                var queueMessage = this.SerializeCloudQueueMessage(queueName, message, dataSerializer);

                var task = Task.Factory.StartNew(() => this.PutRawMessage(queueMessage, queue, timeToLive, delay));
                task.ContinueWith(
                    obj => this.NotifySucceeded(StorageOperationType.QueuePut, stopwatch), 
                    TaskContinuationOptions.OnlyOnRanToCompletion);

                tasks.Add(task);
            }

            try
            {
                Task.WaitAll(tasks.ToArray());
            }
            catch (AggregateException ae)
            {
                throw ae.Flatten();
            }
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
            // 1. GET PERSISTED MESSAGE BLOB
            var blobReference = new PersistedMessageBlobName(storeName, key);
            var blob = this.blobStorage.GetBlob(blobReference);
            if (!blob.HasValue)
            {
                return;
            }

            var persistedMessage = blob.Value;

            // 2. PUT MESSAGE TO QUEUE
            var queue = this.queueStorage.GetQueueReference(persistedMessage.QueueName);
            var rawMessage = new CloudQueueMessage(persistedMessage.Data);
            this.PutRawMessage(rawMessage, queue, timeToLive, delay);

            // 3. DELETE PERSISTED MESSAGE
            this.blobStorage.DeleteBlobIfExist(blobReference);
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
            string queueName;

            lock (this.sync)
            {
                // ignoring message if already deleted
                InProcessMessage inProcMsg;
                if (!this.inProcessMessages.TryGetValue(message, out inProcMsg))
                {
                    return false;
                }

                queueName = inProcMsg.QueueName;
            }

            this.Put(queueName, message, timeToLive, delay);
            return this.Delete(message);
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
            return messages.Count(m => this.ResumeLater(m, timeToLive, delay));
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
            var candidates =
                this.blobStorage.ListBlobNames(ResilientLeasesContainerName).Where(
                    name => !this.blobStorage.IsBlobLocked(ResilientLeasesContainerName, name)).Take(50).ToList();

            var messagesByQueue = new Dictionary<string, int>();
            foreach (var blobName in candidates)
            {
                var lease = this.blobStorage.TryAcquireLease(ResilientLeasesContainerName, blobName);
                if (!lease.IsSuccess)
                {
                    continue;
                }

                try
                {
                    var messageBlob = this.blobStorage.GetBlob<ResilientMessageData>(
                        ResilientMessagesContainerName, blobName);
                    if (!messageBlob.HasValue)
                    {
                        continue;
                    }

                    // CASE: we were able to acquire a lease and can read the original message blob.
                    // => Restore the message
                    var messageData = messageBlob.Value;
                    var queue = this.queueStorage.GetQueueReference(messageData.QueueName);
                    var rawMessage = new CloudQueueMessage(messageData.Data);
                    this.PutRawMessage(rawMessage, queue, timeToLive, delay);

                    if (this.DeleteKeepAliveMessage(blobName, lease.Value))
                    {
                        int oldCount;
                        if (messagesByQueue.TryGetValue(messageData.QueueName, out oldCount))
                        {
                            messagesByQueue[messageData.QueueName] = oldCount + 1;
                        }
                        else
                        {
                            messagesByQueue[messageData.QueueName] = 1;
                        }
                    }
                }
                finally
                {
                    var name = blobName;
                    Retry.DoUntilTrue(
                        this.policies.OptimisticConcurrency, 
                        CancellationToken.None, 
                        () =>
                            {
                                var result = this.blobStorage.TryReleaseLease(
                                    ResilientLeasesContainerName, name, lease.Value);
                                return result.IsSuccess || result.Error == "NotFound" || result.Error == "Conflict";
                            });
                }
            }

            if (this.observer != null && messagesByQueue.Count > 0)
            {
                this.observer.Notify(new MessagesRevivedEvent(messagesByQueue));
            }

            return messagesByQueue.Sum(p => p.Value);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Checks the in message.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void CheckInMessage(object message)
        {
            lock (this.sync)
            {
                var inProcessMessage = this.inProcessMessages[message];
                inProcessMessage.RawMessages.RemoveAt(0);

                if (0 == inProcessMessage.RawMessages.Count)
                {
                    this.inProcessMessages.Remove(message);
                }
            }
        }

        /// <summary>
        /// Checks the out message.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="rawMessage">
        /// The raw message. 
        /// </param>
        /// <param name="data">
        /// The data. 
        /// </param>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="isOverflowing">
        /// if set to <c>true</c> [is overflowing]. 
        /// </param>
        /// <param name="dequeueCount">
        /// The dequeue count. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void CheckOutMessage(
            object message, 
            CloudQueueMessage rawMessage, 
            byte[] data, 
            string queueName, 
            bool isOverflowing, 
            int dequeueCount, 
            IDataSerializer serializer)
        {
            lock (this.sync)
            {
                // If T is a value type, _inprocess could already contain the message
                // (not the same exact instance, but an instance that is value-equal to this one)
                InProcessMessage inProcessMessage;
                if (!this.inProcessMessages.TryGetValue(message, out inProcessMessage))
                {
                    inProcessMessage = new InProcessMessage
                        {
                            QueueName = queueName, 
                            RawMessages = new List<CloudQueueMessage> { rawMessage }, 
                            Serializer = serializer, 
                            Data = data, 
                            IsOverflowing = isOverflowing, 
                            DequeueCount = dequeueCount
                        };
                    this.inProcessMessages.Add(message, inProcessMessage);
                }
                else
                {
                    inProcessMessage.RawMessages.Add(rawMessage);
                }
            }
        }

        /// <summary>
        /// Checks the out relink.
        /// </summary>
        /// <param name="originalMessage">
        /// The original message. 
        /// </param>
        /// <param name="newMessage">
        /// The new message. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void CheckOutRelink(object originalMessage, object newMessage)
        {
            lock (this.sync)
            {
                var inProcessMessage = this.inProcessMessages[originalMessage];
                this.inProcessMessages.Remove(originalMessage);
                this.inProcessMessages.Add(newMessage, inProcessMessage);
            }
        }

        /// <summary>
        /// Deletes the keep alive message.
        /// </summary>
        /// <param name="blobName">
        /// Name of the BLOB. 
        /// </param>
        /// <param name="blobLease">
        /// The BLOB lease. 
        /// </param>
        /// <returns>
        /// The delete keep alive message.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private bool DeleteKeepAliveMessage(string blobName, string blobLease)
        {
            var deleted = false;
            this.blobStorage.DeleteBlobIfExist(ResilientMessagesContainerName, blobName);
            Retry.DoUntilTrue(
                this.policies.OptimisticConcurrency, 
                CancellationToken.None, 
                () =>
                    {
                        var result = this.blobStorage.TryReleaseLease(
                            ResilientLeasesContainerName, blobName, blobLease);
                        if (result.IsSuccess)
                        {
                            deleted = this.blobStorage.DeleteBlobIfExist(ResilientLeasesContainerName, blobName);
                            return true;
                        }

                        if (result.Error == "NotFound")
                        {
                            return true;
                        }

                        if (result.Error == "Conflict")
                        {
                            // CASE: either conflict by another lease (e.g. ReviveMessages), or because it is not leased anymore
                            // => try to delete and retry.
                            // -> if it is not leased anymore, then delete will work and we're done;if not, we need to retry anyway
                            // -> if it is locked by another lease, then the delete will fail with a storage exception, causing a retry
                            deleted = this.blobStorage.DeleteBlobIfExist(ResilientLeasesContainerName, blobName);
                            return false;
                        }

                        return false;
                    });

            return deleted;
        }

        /// <summary>
        /// Deletes the overflowing messages.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void DeleteOverflowingMessages(string queueName)
        {
            this.blobStorage.DeleteAllBlobs(OverflowingMessagesContainerName, queueName);
        }

        /// <summary>
        /// Deletes the raw message.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="queue">
        /// The queue. 
        /// </param>
        /// <returns>
        /// The delete raw message.
        /// </returns>
        /// <remarks>
        /// </remarks>
        private bool DeleteRawMessage(CloudQueueMessage message, CloudQueue queue)
        {
            try
            {
                Retry.Do(
                    this.policies.TransientServerErrorBackOff, 
                    CancellationToken.None, 
                    () => queue.DeleteMessage(message));
                return true;
            }
            catch (StorageClientException ex)
            {
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }

                var info = ex.ExtendedErrorInformation;
                if (info == null)
                {
                    throw;
                }

                if (info.ErrorCode == QueueErrorCodeStrings.PopReceiptMismatch)
                {
                    return false;
                }

                if (info.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    return false;
                }

                throw;
            }
        }

        /// <summary>
        /// Notifies the succeeded.
        /// </summary>
        /// <param name="operationType">
        /// Type of the operation. 
        /// </param>
        /// <param name="stopwatch">
        /// The stopwatch. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void NotifySucceeded(StorageOperationType operationType, Stopwatch stopwatch)
        {
            if (this.observer != null)
            {
                this.observer.Notify(new StorageOperationSucceededEvent(operationType, stopwatch.Elapsed));
            }
        }

        /// <summary>
        /// Persists the raw message.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="data">
        /// The data. 
        /// </param>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <param name="reason">
        /// The reason. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void PersistRawMessage(
            CloudQueueMessage message, byte[] data, string queueName, string storeName, string reason)
        {
            var stopwatch = Stopwatch.StartNew();

            var queue = this.queueStorage.GetQueueReference(queueName);

            // 1. PERSIST MESSAGE TO BLOB
            var blobReference = PersistedMessageBlobName.GetNew(storeName);
            Debug.Assert(message.InsertionTime != null, "message.InsertionTime != null");
            var persistedMessage = new PersistedMessageData
                {
                    QueueName = queueName, 
                    InsertionTime = message.InsertionTime.Value, 
                    PersistenceTime = DateTimeOffset.UtcNow, 
                    DequeueCount = message.DequeueCount, 
                    Reason = reason, 
                    Data = data, 
                };

            this.blobStorage.PutBlob(blobReference, persistedMessage);

            // 2. DELETE MESSAGE FROM QUEUE
            this.DeleteRawMessage(message, queue);

            this.NotifySucceeded(StorageOperationType.QueuePersist, stopwatch);
        }

        /// <summary>
        /// Puts the overflowing message and wrap.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private byte[] PutOverflowingMessageAndWrap<T>(string queueName, T message, IDataSerializer serializer)
        {
            var stopwatch = Stopwatch.StartNew();

            var blobRef = OverflowingMessageBlobName<T>.GetNew(queueName);

            // HACK: In this case serialization is performed another time (internally)
            this.blobStorage.PutBlob(blobRef, message);

            var mw = new MessageWrapper { ContainerName = blobRef.ContainerName, BlobName = blobRef.ToString() };

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(mw, stream, typeof(MessageWrapper));
                var serializerWrapper = stream.ToArray();

                this.NotifySucceeded(StorageOperationType.QueueWrap, stopwatch);

                return serializerWrapper;
            }
        }

        /// <summary>
        /// Puts the raw message.
        /// </summary>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="queue">
        /// The queue. 
        /// </param>
        /// <param name="timeToLive">
        /// The time to live. 
        /// </param>
        /// <param name="delay">
        /// The delay. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private void PutRawMessage(CloudQueueMessage message, CloudQueue queue, TimeSpan timeToLive, TimeSpan delay)
        {
            var ttlOrNot = timeToLive < CloudQueueMessage.MaxTimeToLive && timeToLive > TimeSpan.Zero
                               ? timeToLive
                               : new TimeSpan?();
            var delayOrNot = delay < CloudQueueMessage.MaxTimeToLive && delay > TimeSpan.Zero ? delay : new TimeSpan?();

            try
            {
                Retry.Do(
                    this.policies.TransientServerErrorBackOff, 
                    CancellationToken.None, 
                    () => queue.AddMessage(message, ttlOrNot, delayOrNot));
            }
            catch (StorageClientException ex)
            {
                // HACK: not storage status error code yet
                if (ex.ErrorCode == StorageErrorCode.ResourceNotFound
                    || ex.ExtendedErrorInformation.ErrorCode == QueueErrorCodeStrings.QueueNotFound)
                {
                    // It usually takes time before the queue gets available
                    // (the queue might also have been freshly deleted).
                    Retry.Do(
                        this.policies.SlowInstantiation, 
                        CancellationToken.None, 
                        () =>
                            {
                                queue.Create();
                                queue.AddMessage(message);
                            });
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Serializes the cloud queue message.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="message">
        /// The message. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private CloudQueueMessage SerializeCloudQueueMessage<T>(string queueName, T message, IDataSerializer serializer)
        {
            CloudQueueMessage queueMessage;
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(message, stream, typeof(T));

                // Caution: MaxMessageSize is not related to the number of bytes
                // but the number of characters when Base64-encoded:
                if (stream.Length >= (CloudQueueMessage.MaxMessageSize - 1) / 4 * 3)
                {
                    queueMessage =
                        new CloudQueueMessage(this.PutOverflowingMessageAndWrap(queueName, message, serializer));
                }
                else
                {
                    try
                    {
                        queueMessage = new CloudQueueMessage(stream.ToArray());
                    }
                    catch (ArgumentException)
                    {
                        queueMessage =
                            new CloudQueueMessage(this.PutOverflowingMessageAndWrap(queueName, message, serializer));
                    }
                }
            }

            return queueMessage;
        }

        #endregion
    }

    /// <summary>
    /// The identity comparer.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal class IdentityComparer : IEqualityComparer<object>
    {
        #region Public Methods and Operators

        /// <summary>
        /// Determines whether this instance [can differentiate instances] the specified type.
        /// </summary>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// <c>true</c> if this instance [can differentiate instances] the specified type; otherwise, <c>false</c> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static bool CanDifferentiateInstances(Type type)
        {
            return type != typeof(string) && type.IsClass;
        }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">
        /// The first object of type T to compare. 
        /// </param>
        /// <param name="y">
        /// The second object of type T to compare. 
        /// </param>
        /// <returns>
        /// true if the specified objects are equal; otherwise, false. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public new bool Equals(object x, object y)
        {
            if (x == null)
            {
                return y == null;
            }

            if (y == null)
            {
                return false;
            }

            return x.GetType().IsClass ? ReferenceEquals(x, y) : x.Equals(y);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">
        /// The <see cref="T:System.Object"></see> for which a hash code is to be returned. 
        /// </param>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        /// <exception cref="T:System.ArgumentNullException">
        /// The type of obj is a reference type and obj is null.
        /// </exception>
        /// <remarks>
        /// </remarks>
        public int GetHashCode(object obj)
        {
            return obj == null ? 0 : obj.GetHashCode();
        }

        #endregion
    }

    /// <summary>
    /// Represents a set of value-identical messages that are being processed by workers, i.e. were hidden from the queue because of calls to Get{T}.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal class InProcessMessage
    {
        #region Public Properties

        /// <summary>
        ///   True if Delete, Abandon or ResumeNext has been requested.
        /// </summary>
        /// <value> <c>true</c> if [commit started]; otherwise, <c>false</c> . </value>
        /// <remarks>
        /// </remarks>
        public bool CommitStarted { get; set; }

        /// <summary>
        ///   The unpacked message data. Can still be a message wrapper, but never an envelope.
        /// </summary>
        /// <value> The data. </value>
        /// <remarks>
        /// </remarks>
        public byte[] Data { get; set; }

        /// <summary>
        ///   The number of times this message has already been dequeued, so we can track it safely even when abandoning it later
        /// </summary>
        /// <value> The dequeue count. </value>
        /// <remarks>
        /// </remarks>
        public int DequeueCount { get; set; }

        /// <summary>
        ///   A flag indicating whether the original message was bigger than the max allowed size and was therefore wrapped in <see
        ///    cref="MessageWrapper" /> .
        /// </summary>
        /// <value> <c>true</c> if this instance is overflowing; otherwise, <c>false</c> . </value>
        /// <remarks>
        /// </remarks>
        public bool IsOverflowing { get; set; }

        /// <summary>
        ///   Gets or sets the keep alive BLOB lease.
        /// </summary>
        /// <value> The keep alive BLOB lease. </value>
        /// <remarks>
        /// </remarks>
        public string KeepAliveBlobLease { get; set; }

        /// <summary>
        ///   Gets or sets the name of the keep alive BLOB.
        /// </summary>
        /// <value> The name of the keep alive BLOB. </value>
        /// <remarks>
        /// </remarks>
        public string KeepAliveBlobName { get; set; }

        /// <summary>
        ///   Gets or sets the keep alive timeout.
        /// </summary>
        /// <value> The keep alive timeout. </value>
        /// <remarks>
        /// </remarks>
        public DateTimeOffset KeepAliveTimeout { get; set; }

        /// <summary>
        ///   Name of the queue where messages are originating from.
        /// </summary>
        /// <value> The name of the queue. </value>
        /// <remarks>
        /// </remarks>
        public string QueueName { get; set; }

        /// <summary>
        ///   The multiple, different raw <see cref="CloudQueueMessage" /> objects as returned from the queue storage.
        /// </summary>
        /// <value> The raw messages. </value>
        /// <remarks>
        /// </remarks>
        public List<CloudQueueMessage> RawMessages { get; set; }

        /// <summary>
        ///   Serializer used for this message.
        /// </summary>
        /// <value> The serializer. </value>
        /// <remarks>
        /// </remarks>
        public IDataSerializer Serializer { get; set; }

        #endregion
    }

    /// <summary>
    /// The overflowing message blob name.
    /// </summary>
    /// <typeparam name="T">
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    internal class OverflowingMessageBlobName<T> : BlobName<T>
    {
        #region Constants and Fields

        /// <summary>
        ///   Message identifiers as specified by the queue storage itself.
        /// </summary>
        [Rank(1)]
        public Guid MessageId;

        /// <summary>
        ///   Indicates the name of the queue where the message has been originally pushed.
        /// </summary>
        [Rank(0)]
        public string QueueName;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="OverflowingMessageBlobName{T}"/> class. 
        /// Prevents a default instance of the <see cref="OverflowingMessageBlobName&lt;T&gt;"/> class from being created.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <param name="guid">
        /// The GUID. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private OverflowingMessageBlobName(string queueName, Guid guid)
        {
            this.QueueName = queueName;
            this.MessageId = guid;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Name of the container where the blob is located.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public override string ContainerName
        {
            get
            {
                return QueueStorageProvider.OverflowingMessagesContainerName;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Used to iterate over all the overflowing messages associated to a queue.
        /// </summary>
        /// <param name="queueName">
        /// Name of the queue. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static OverflowingMessageBlobName<T> GetNew(string queueName)
        {
            return new OverflowingMessageBlobName<T>(queueName, Guid.NewGuid());
        }

        #endregion
    }

    /// <summary>
    /// The persisted message data.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    [Serializable]
    internal class PersistedMessageData
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the data.
        /// </summary>
        /// <value> The data. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 6)]
        public byte[] Data { get; set; }

        /// <summary>
        ///   Gets or sets the dequeue count.
        /// </summary>
        /// <value> The dequeue count. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 4)]
        public int DequeueCount { get; set; }

        /// <summary>
        ///   Gets or sets the insertion time.
        /// </summary>
        /// <value> The insertion time. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public DateTimeOffset InsertionTime { get; set; }

        /// <summary>
        ///   Gets or sets the persistence time.
        /// </summary>
        /// <value> The persistence time. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 3)]
        public DateTimeOffset PersistenceTime { get; set; }

        /// <summary>
        ///   Gets or sets the name of the queue.
        /// </summary>
        /// <value> The name of the queue. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public string QueueName { get; set; }

        /// <summary>
        ///   Gets or sets the reason.
        /// </summary>
        /// <value> The reason. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 5, IsRequired = false)]
        public string Reason { get; set; }

        #endregion
    }

    /// <summary>
    /// The resilient message data.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    [Serializable]
    internal class ResilientMessageData
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the data.
        /// </summary>
        /// <value> The data. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public byte[] Data { get; set; }

        /// <summary>
        ///   Gets or sets the name of the queue.
        /// </summary>
        /// <value> The name of the queue. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public string QueueName { get; set; }

        #endregion
    }

    /// <summary>
    /// The resilient lease data.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    internal class ResilientLeaseData
    {
        #region Public Properties

        /// <summary>
        ///   Gets or sets the name of the BLOB.
        /// </summary>
        /// <value> The name of the BLOB. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 2)]
        public string BlobName { get; set; }

        /// <summary>
        ///   Gets or sets the name of the queue.
        /// </summary>
        /// <value> The name of the queue. </value>
        /// <remarks>
        /// </remarks>
        [DataMember(Order = 1)]
        public string QueueName { get; set; }

        #endregion
    }

    /// <summary>
    /// The persisted message blob name.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal class PersistedMessageBlobName : BlobName<PersistedMessageData>
    {
        #region Constants and Fields

        /// <summary>
        /// The key.
        /// </summary>
        [Rank(1)]
        public string Key;

        /// <summary>
        ///   Indicates the name of the swap out store where the message is persisted.
        /// </summary>
        [Rank(0)]
        public string StoreName;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="PersistedMessageBlobName"/> class.
        /// </summary>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public PersistedMessageBlobName(string storeName, string key)
        {
            this.StoreName = storeName;
            this.Key = key;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Name of the container where the blob is located.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public override string ContainerName
        {
            get
            {
                return "lokad-cloud-persisted-messages";
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Gets the new.
        /// </summary>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <param name="key">
        /// The key. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static PersistedMessageBlobName GetNew(string storeName, string key)
        {
            return new PersistedMessageBlobName(storeName, key);
        }

        /// <summary>
        /// Gets the new.
        /// </summary>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static PersistedMessageBlobName GetNew(string storeName)
        {
            return new PersistedMessageBlobName(storeName, Guid.NewGuid().ToString("N"));
        }

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        /// <param name="storeName">
        /// Name of the store. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static PersistedMessageBlobName GetPrefix(string storeName)
        {
            return new PersistedMessageBlobName(storeName, null);
        }

        #endregion
    }
}