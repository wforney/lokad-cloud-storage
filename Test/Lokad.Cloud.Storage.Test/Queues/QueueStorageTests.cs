#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Queues
{
    using System;
    using System.Linq;
    using System.Text;

    using Lokad.Cloud.Storage.Blobs;
    using Lokad.Cloud.Storage.Queues;

    using NUnit.Framework;

    /// <summary>
    /// The queue storage tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public abstract class QueueStorageTests
    {
        #region Constants and Fields

        /// <summary>
        /// The blob storage.
        /// </summary>
        protected readonly IBlobStorageProvider BlobStorage;

        /// <summary>
        /// The queue storage.
        /// </summary>
        protected readonly IQueueStorageProvider QueueStorage;

        /// <summary>
        /// The queue name.
        /// </summary>
        protected string QueueName;

        /// <summary>
        /// The base queue name.
        /// </summary>
        private const string BaseQueueName = "tests-queuestorageprovider-";

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorageTests"/> class.
        /// </summary>
        /// <param name="storage">
        /// The storage. 
        /// </param>
        /// <remarks>
        /// </remarks>
        protected QueueStorageTests(CloudStorageProviders storage)
        {
            this.QueueStorage = storage.QueueStorage;
            this.BlobStorage = storage.BlobStorage;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Persists the restore.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PersistRestore()
        {
            const string StoreName = "TestStore";

            var message = new MyMessage();

            // clean up
            this.QueueStorage.DeleteQueue(this.QueueName);
            foreach (var skey in this.QueueStorage.ListPersisted(StoreName))
            {
                this.QueueStorage.DeletePersisted(StoreName, skey);
            }

            // put
            this.QueueStorage.Put(this.QueueName, message);

            // get
            var retrieved = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();
            Assert.AreEqual(message.MyGuid, retrieved.MyGuid, "#A01");

            // persist
            this.QueueStorage.Persist(retrieved, StoreName, "manual test");

            // abandon should fail (since not invisible anymore)
            Assert.IsFalse(this.QueueStorage.Abandon(retrieved), "#A02");

            // list persisted message
            var key = this.QueueStorage.ListPersisted(StoreName).Single();

            // get persisted message
            var persisted = this.QueueStorage.GetPersisted(StoreName, key);
            Assert.IsTrue(persisted.HasValue, "#A03");
            Assert.IsTrue(persisted.Value.DataXml.HasValue, "#A04");
            var xml = persisted.Value.DataXml.Value;
            var property = xml.Elements().Single(x => x.Name.LocalName == "MyGuid");
            Assert.AreEqual(message.MyGuid, new Guid(property.Value), "#A05");

            // restore persisted message
            this.QueueStorage.RestorePersisted(StoreName, key);

            // list no longer contains key
            Assert.IsFalse(this.QueueStorage.ListPersisted(StoreName).Any(), "#A06");

            // get
            var retrieved2 = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();
            Assert.AreEqual(message.MyGuid, retrieved2.MyGuid, "#A07");

            // delete
            Assert.IsTrue(this.QueueStorage.Delete(retrieved2), "#A08");
        }

        /// <summary>
        /// Puts the get abandon delete.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutGetAbandonDelete()
        {
            var message = new MyMessage();

            this.QueueStorage.DeleteQueue(this.QueueName); // deleting queue on purpose 

            // (it's slow but necessary to really validate the retry policy)

            // put
            this.QueueStorage.Put(this.QueueName, message);

            // get
            var retrieved = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();
            Assert.AreEqual(message.MyGuid, retrieved.MyGuid, "#A01");

            // abandon
            var abandoned = this.QueueStorage.Abandon(retrieved);
            Assert.IsTrue(abandoned, "#A02");

            // abandon II should fail (since not invisible)
            var abandoned2 = this.QueueStorage.Abandon(retrieved);
            Assert.IsFalse(abandoned2, "#A03");

            // get again
            var retrieved2 = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();
            Assert.AreEqual(message.MyGuid, retrieved2.MyGuid, "#A04");

            // delete
            var deleted = this.QueueStorage.Delete(retrieved2);
            Assert.IsTrue(deleted, "#A05");

            // get now should fail
            var retrieved3 = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).FirstOrDefault();
            Assert.IsNull(retrieved3, "#A06");

            // abandon does not put it to the queue again
            var abandoned3 = this.QueueStorage.Abandon(retrieved2);
            Assert.IsFalse(abandoned3, "#A07");

            // get now should still fail
            var retrieved4 = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).FirstOrDefault();
            Assert.IsNull(retrieved4, "#A07");
        }

        /// <summary>
        /// Puts the get delete.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutGetDelete()
        {
            var message = new MyMessage();

            this.QueueStorage.DeleteQueue(this.QueueName); // deleting queue on purpose 

            // (it's slow but necessary to really validate the retry policy)
            this.QueueStorage.Put(this.QueueName, message);
            var retrieved = this.QueueStorage.Get<MyMessage>(this.QueueName, 1).First();

            Assert.AreEqual(message.MyGuid, retrieved.MyGuid, "#A01");

            this.QueueStorage.Delete(retrieved);
        }

        /// <summary>
        /// Puts the get delete identical struct or native.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PutGetDeleteIdenticalStructOrNative()
        {
            var testStruct = new MyStruct { IntegerValue = 12, StringValue = "hello" };

            for (var i = 0; i < 10; i++)
            {
                this.QueueStorage.Put(this.QueueName, testStruct);
            }

            var outStruct1 = this.QueueStorage.Get<MyStruct>(this.QueueName, 1).First();
            var outStruct2 = this.QueueStorage.Get<MyStruct>(this.QueueName, 1).First();
            Assert.IsTrue(this.QueueStorage.Delete(outStruct1), "1st Delete failed");
            Assert.IsTrue(this.QueueStorage.Delete(outStruct2), "2nd Delete failed");
            Assert.IsFalse(this.QueueStorage.Delete(outStruct2), "3nd Delete succeeded");

            var outAllStructs = this.QueueStorage.Get<MyStruct>(this.QueueName, 20);
            Assert.AreEqual(8, outAllStructs.Count(), "Wrong queue item count");
            foreach (var str in outAllStructs)
            {
                Assert.AreEqual(testStruct.IntegerValue, str.IntegerValue, "Wrong integer value");
                Assert.AreEqual(testStruct.StringValue, str.StringValue, "Wrong string value");
                Assert.IsTrue(this.QueueStorage.Delete(str), "Delete failed");
            }

            const double TestDouble = 3.6D;

            for (var i = 0; i < 10; i++)
            {
                this.QueueStorage.Put(this.QueueName, TestDouble);
            }

            var outDouble1 = this.QueueStorage.Get<double>(this.QueueName, 1).First();
            var outDouble2 = this.QueueStorage.Get<double>(this.QueueName, 1).First();
            var outDouble3 = this.QueueStorage.Get<double>(this.QueueName, 1).First();
            Assert.IsTrue(this.QueueStorage.Delete(outDouble1), "1st Delete failed");
            Assert.IsTrue(this.QueueStorage.Delete(outDouble2), "2nd Delete failed");
            Assert.IsTrue(this.QueueStorage.Delete(outDouble3), "3nd Delete failed");
            Assert.IsFalse(this.QueueStorage.Delete(outDouble2), "3nd Delete succeeded");

            var outAllDoubles = this.QueueStorage.Get<double>(this.QueueName, 20);
            Assert.AreEqual(7, outAllDoubles.Count(), "Wrong queue item count");
            foreach (var dbl in outAllDoubles)
            {
                Assert.AreEqual(TestDouble, dbl, "Wrong double value");
                Assert.IsTrue(this.QueueStorage.Delete(dbl), "Delete failed");
            }

            const string TestString = "hi there!";

            for (var i = 0; i < 10; i++)
            {
                this.QueueStorage.Put(this.QueueName, TestString);
            }

            var outString1 = this.QueueStorage.Get<string>(this.QueueName, 1).First();
            var outString2 = this.QueueStorage.Get<string>(this.QueueName, 1).First();
            Assert.IsTrue(this.QueueStorage.Delete(outString1), "1st Delete failed");
            Assert.IsTrue(this.QueueStorage.Delete(outString2), "2nd Delete failed");
            Assert.IsFalse(this.QueueStorage.Delete(outString2), "3nd Delete succeeded");

            var outAllStrings = this.QueueStorage.Get<string>(this.QueueName, 20);
            Assert.AreEqual(8, outAllStrings.Count(), "Wrong queue item count");
            foreach (var str in outAllStrings)
            {
                Assert.AreEqual(TestString, str, "Wrong string value");
                Assert.IsTrue(this.QueueStorage.Delete(str), "Delete failed");
            }

            var testClass = new StringBuilder("text");

            for (var i = 0; i < 10; i++)
            {
                this.QueueStorage.Put(this.QueueName, testClass);
            }

            var outClass1 = this.QueueStorage.Get<StringBuilder>(this.QueueName, 1).First();
            var outClass2 = this.QueueStorage.Get<StringBuilder>(this.QueueName, 1).First();
            Assert.IsTrue(this.QueueStorage.Delete(outClass1), "1st Delete failed");
            Assert.IsTrue(this.QueueStorage.Delete(outClass2), "2nd Delete failed");
            Assert.IsFalse(this.QueueStorage.Delete(outClass2), "3nd Delete succeeded");

            var outAllClasses = this.QueueStorage.Get<StringBuilder>(this.QueueName, 20);
            Assert.AreEqual(8, outAllClasses.Count(), "Wrong queue item count");
            foreach (var cls in outAllClasses)
            {
                Assert.AreEqual(testClass.ToString(), cls.ToString(), "Wrong deserialized class value");
                Assert.IsTrue(this.QueueStorage.Delete(cls), "Delete failed");
            }
        }

        /// <summary>
        /// Setups this instance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [SetUp]
        public void Setup()
        {
            this.QueueName = BaseQueueName + Guid.NewGuid().ToString("N");
        }

        /// <summary>
        /// Tears down.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TearDown]
        public virtual void TearDown()
        {
            this.QueueStorage.DeleteQueue(this.QueueName);
        }

        #endregion

        // TODO: create same unit test for Clear()
    }
}