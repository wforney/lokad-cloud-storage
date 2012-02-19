#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Queues
{
    using System;
    using System.Linq;

    using Lokad.Cloud.Storage.Queues;

    using NUnit.Framework;

    /// <summary>
    /// The memory queue storage tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    [Category("InMemoryStorage")]
    public class MemoryQueueStorageTests : QueueStorageTests
    {
        #region Constants and Fields

        /// <summary>
        /// The first queue name.
        /// </summary>
        private const string FirstQueueName = "firstQueueName";

        /// <summary>
        /// The second queue name.
        /// </summary>
        private const string SecondQueueName = "secondQueueName";

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryQueueStorageTests"/> class. 
        ///   Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryQueueStorageTests()
            : base(CloudStorage.ForInMemoryStorage().BuildStorageProviders())
        {
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Gets the on missing queue does not fail.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void GetOnMissingQueueDoesNotFail()
        {
            this.QueueStorage.Get<int>("nosuchqueue", 1);
        }

        /// <summary>
        /// Itemses the get put in mono thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ItemsGetPutInMonoThread()
        {
            var fakeMessages = Enumerable.Range(0, 3).Select(i => new FakeMessage(i)).ToArray();

            this.QueueStorage.PutRange(FirstQueueName, fakeMessages.Take(2));
            this.QueueStorage.PutRange(SecondQueueName, fakeMessages.Skip(2).ToArray());

            Assert.AreEqual(
                2, 
                this.QueueStorage.GetApproximateCount(FirstQueueName), 
                "#A04 First queue has not the right number of elements.");
            Assert.AreEqual(
                1, 
                this.QueueStorage.GetApproximateCount(SecondQueueName), 
                "#A05 Second queue has not the right number of elements.");
        }

        /// <summary>
        /// Itemses the returned in mono thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ItemsReturnedInMonoThread()
        {
            var fakeMessages = Enumerable.Range(0, 10).Select(i => new FakeMessage(i)).ToArray();

            this.QueueStorage.PutRange(FirstQueueName, fakeMessages.Take(6));
            var allFirstItems = this.QueueStorage.Get<FakeMessage>(FirstQueueName, 6);
            this.QueueStorage.Clear(FirstQueueName);

            this.QueueStorage.PutRange(FirstQueueName, fakeMessages.Take(6));
            var partOfFirstItems = this.QueueStorage.Get<FakeMessage>(FirstQueueName, 2);
            Assert.AreEqual(4, this.QueueStorage.GetApproximateCount(FirstQueueName), "#A06");
            this.QueueStorage.Clear(FirstQueueName);

            this.QueueStorage.PutRange(FirstQueueName, fakeMessages.Take(6));
            var allFirstItemsAndMore = this.QueueStorage.Get<FakeMessage>(FirstQueueName, 8);
            this.QueueStorage.Clear(FirstQueueName);

            Assert.AreEqual(6, allFirstItems.Count(), "#A07");
            Assert.AreEqual(2, partOfFirstItems.Count(), "#A08");
            Assert.AreEqual(6, allFirstItemsAndMore.Count(), "#A09");
        }

        /// <summary>
        /// Lists the in mono thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ListInMonoThread()
        {
            var fakeMessages = Enumerable.Range(0, 10).Select(i => new FakeMessage(i)).ToArray();

            this.QueueStorage.PutRange(FirstQueueName, fakeMessages.Take(6));
            var queuesName = this.QueueStorage.List(string.Empty);

            Assert.AreEqual(1, queuesName.Count(), "#A010");
        }

        /// <summary>
        /// Tears down.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
            this.QueueStorage.DeleteQueue(FirstQueueName);
            this.QueueStorage.DeleteQueue(SecondQueueName);
        }

        #endregion

        /// <summary>
        /// The fake message.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Serializable]
        private class FakeMessage
        {
            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="FakeMessage"/> class.
            /// </summary>
            /// <param name="value">
            /// The value. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public FakeMessage(double value)
            {
                this.Value = value;
            }

            #endregion

            #region Properties

            /// <summary>
            ///   Gets or sets the value.
            /// </summary>
            /// <value> The value. </value>
            /// <remarks>
            /// </remarks>
            private double Value { get; set; }

            #endregion
        }
    }
}