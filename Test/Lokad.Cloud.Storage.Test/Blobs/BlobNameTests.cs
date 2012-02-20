#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Blobs
{
    using System;

    using Lokad.Cloud.Storage.Blobs;

    using NUnit.Framework;

    /// <summary>
    /// The blob name tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public class BlobNameTests
    {
        // ReSharper disable InconsistentNaming
        #region Public Methods and Operators

        /// <summary>
        /// Conversion_round_trips this instance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Conversion_round_trip()
        {
            var date = new DateTime(2009, 1, 1, 3, 4, 5);
            var original = new PatternA(date, 12000, Guid.NewGuid(), 120);

            var name = UntypedBlobName.Print(original);

            // Console.WriteLine(name);
            var parsed = UntypedBlobName.Parse<PatternA>(name);
            Assert.AreNotSame(original, parsed);
            Assert.AreEqual(original.Timestamp, parsed.Timestamp);
            Assert.AreEqual(original.AccountHRID, parsed.AccountHRID);
            Assert.AreEqual(original.ChunkID, parsed.ChunkID);
            Assert.AreEqual(original.ChunkSize, parsed.ChunkSize);
        }

        /// <summary>
        /// Conversion_round_trip_s the nullable.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Conversion_round_trip_Nullable()
        {
            var original1 = new PatternH { LongId = 0, IntId = 0 };
            var name1 = UntypedBlobName.Print(original1);
            var parsed1 = UntypedBlobName.Parse<PatternH>(name1);

            Assert.AreNotSame(original1, parsed1);
            Assert.AreEqual(original1.LongId, parsed1.LongId);
            Assert.AreEqual(original1.IntId, parsed1.IntId);

            var original2 = new PatternH { LongId = 10, IntId = 20 };
            var name2 = UntypedBlobName.Print(original2);
            var parsed2 = UntypedBlobName.Parse<PatternH>(name2);

            Assert.AreNotSame(original2, parsed2);
            Assert.AreEqual(original2.LongId, parsed2.LongId);
            Assert.AreEqual(original2.IntId, parsed2.IntId);
        }

        /// <summary>
        /// Field_s the order_ matters.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Field_Order_Matters()
        {
            var g = Guid.NewGuid();
            var pb = new PatternB(g, 1000);
            var pc = new PatternC(g, 1000);

            Assert.AreNotEqual(pb.ToString(), pc.ToString());
        }

        /// <summary>
        /// Field_s the order_ works_ with_ inheritance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Field_Order_Works_With_Inheritance()
        {
            var g = Guid.NewGuid();
            var pc = new PatternC(g, 1000);
            var pd = new PatternD(g, 1000, 1234);

            Assert.IsTrue(pd.ToString().StartsWith(pc.ToString()));
        }

        /// <summary>
        /// Partials the print_ manage_ default time value.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PartialPrint_Manage_DefaultTimeValue()
        {
            var pattern = new PatternA();
            Assert.AreEqual(string.Empty, pattern.ToString());
        }

        /// <summary>
        /// Partials the print_ manage_ empty GUID.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PartialPrint_Manage_EmptyGuid()
        {
            var date = new DateTime(2009, 1, 1, 3, 4, 5);
            var pattern = new PatternA(date, 12000, Guid.Empty, 120);
            Assert.IsTrue(!pattern.ToString().EndsWith(120.ToString()));
        }

        /// <summary>
        /// Partials the print_ manage_ nullable.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void PartialPrint_Manage_Nullable()
        {
            Assert.AreEqual("10/20", UntypedBlobName.Print(new PatternH { LongId = 10, IntId = 20 }));
            Assert.AreEqual("10/", UntypedBlobName.Print(new PatternH { LongId = 10, IntId = null }));
            Assert.AreEqual(string.Empty, UntypedBlobName.Print(new PatternH { LongId = null, IntId = null }));
            Assert.AreEqual(string.Empty, UntypedBlobName.Print(new PatternH { LongId = null, IntId = 20 }));
        }

        /// <summary>
        /// Properties_s the should_ work.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Properties_Should_Work()
        {
            var pattern = new PatternF { AccountHRID = 17, ChunkID = Guid.NewGuid() };
            Assert.AreEqual(pattern.AccountHRID + "/" + pattern.ChunkID.ToString("N"), pattern.ToString());
        }

        /// <summary>
        /// Time_zone_safe_when_using_s the date time offset.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Time_zone_safe_when_using_DateTimeOffset()
        {
            var localOffset = TimeSpan.FromHours(-2);
            var now = DateTimeOffset.Now;

            // round to seconds, our time resolution in blob names
            now = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Offset);
            var unsafeNow = now.DateTime;

            var utcNow = now.ToUniversalTime();
            var localNow = now.ToOffset(localOffset);
            var unsafeUtcNow = utcNow.UtcDateTime;
            var unsafeLocalNow = localNow.DateTime;

            // 'dummy' argument set as 'null'
            var localString = UntypedBlobName.Print(new PatternE(unsafeLocalNow, localNow, null));
            var localName = UntypedBlobName.Parse<PatternE>(localString);

            Assert.AreEqual(now, localName.AbsoluteTime, "DateTimeOffset-local");
            Assert.AreEqual(utcNow, localName.AbsoluteTime, "DateTimeOffset-local-utc");
            Assert.AreEqual(localNow, localName.AbsoluteTime, "DateTimeOffset-local-local");

            Assert.AreNotEqual(unsafeNow, localName.UserTime, "DateTime-local");
            Assert.AreNotEqual(unsafeUtcNow, localName.UserTime, "DateTime-local-utc");
            Assert.AreEqual(unsafeLocalNow, localName.UserTime, "DateTime-local-local");

            Assert.AreNotEqual(unsafeUtcNow, localName.UserTime, "DateTime-local");

            // 'dummy' argument set as 'null'
            var utcString = UntypedBlobName.Print(new PatternE(unsafeUtcNow, utcNow, null));
            var utcName = UntypedBlobName.Parse<PatternE>(utcString);

            Assert.AreEqual(now, utcName.AbsoluteTime, "DateTimeOffset-utc");
            Assert.AreEqual(utcNow, utcName.AbsoluteTime, "DateTimeOffset-local-utc");
            Assert.AreEqual(localNow, utcName.AbsoluteTime, "DateTimeOffset-local-local");

            if (unsafeNow != unsafeUtcNow)
            {
                // in case current machine runs NOT at UTC time
                Assert.AreNotEqual(unsafeNow, utcName.UserTime, "DateTime-utc");
            }

            Assert.AreEqual(unsafeUtcNow, utcName.UserTime, "DateTime-utc-utc");
            if (unsafeNow != unsafeLocalNow)
            {
                // in case current machine runs NOT at UTC-2 time
                Assert.AreNotEqual(unsafeLocalNow, utcName.UserTime, "DateTime-utc-local");
            }
        }

        /// <summary>
        /// Treats the default as null_supports_datetime.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void TreatDefaultAsNull_supports_datetime()
        {
            var id = DateTime.UtcNow.Date;
            Assert.AreEqual(id.ToString("yyyy-MM-dd-HH-mm-ss"), new PatternG<DateTime>(id).ToString());
            Assert.AreEqual(string.Empty, new PatternG<DateTime>(default(DateTime)).ToString());
            Assert.AreNotEqual(string.Empty, new PatternG<DateTime>(DateTime.Now).ToString());
        }

        /// <summary>
        /// Treats the default as null_supports_guid.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void TreatDefaultAsNull_supports_guid()
        {
            var id = Guid.NewGuid();
            Assert.AreEqual(id.ToString("N"), new PatternG<Guid>(id).ToString());
            Assert.AreEqual(string.Empty, new PatternG<Guid>(Guid.Empty).ToString());
            Assert.AreEqual(string.Empty, new PatternG<Guid>(default(Guid)).ToString());
            Assert.AreNotEqual(string.Empty, new PatternG<Guid>(Guid.NewGuid()).ToString());
        }

        /// <summary>
        /// Treats the default as null_supports_string.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void TreatDefaultAsNull_supports_string()
        {
            var id = Guid.NewGuid().ToString();
            Assert.AreEqual(id, new PatternG<string>(id).ToString());
            Assert.AreEqual(string.Empty, new PatternG<string>(null).ToString());
            Assert.AreEqual(string.Empty, new PatternG<string>(string.Empty).ToString());
            Assert.AreNotEqual(string.Empty, new PatternG<string>("foo").ToString());
        }

        /// <summary>
        /// Two_s the patterns.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Two_Patterns()
        {
            // actually ensures that the implementation supports two patterns
            var date = new DateTime(2009, 1, 1, 3, 4, 5);
            var pa = new PatternA(date, 12000, Guid.NewGuid(), 120);
            var pb = new PatternB(Guid.NewGuid(), 1000);

            Assert.AreNotEqual(pa.ToString(), pb.ToString());
        }

        /// <summary>
        /// Wrong_type_is_detecteds this instance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Wrong_type_is_detected()
        {
            try
            {
                var original = new PatternB(Guid.NewGuid(), 1000);
                var name = UntypedBlobName.Print(original);
                UntypedBlobName.Parse<PatternA>(name);

                Assert.Fail("#A00");
            }
            catch (ArgumentException)
            {
            }
        }

        #endregion

        /// <summary>
        /// The pattern g.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <remarks>
        /// </remarks>
        public class PatternG<T> : BlobName<int>
        {
            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternG{T}"/> class. 
            /// Initializes a new instance of the <see cref="PatternG&lt;T&gt;"/> class.
            /// </summary>
            /// <param name="id">
            /// The id. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternG(T id)
            {
                this.Id = id;
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
                    return "foo";
                }
            }

            /// <summary>
            ///   Gets or sets the id.
            /// </summary>
            /// <value> The id. </value>
            /// <remarks>
            /// </remarks>
            [Rank(0, true)]
            public T Id { get; set; }

            #endregion
        }

        /// <summary>
        /// The pattern a.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternA : BlobName<string>
        {
            // not a field
            #region Constants and Fields

            /// <summary>
            /// The account hrid.
            /// </summary>
            [Rank(1)]
            public readonly long AccountHRID;

            /// <summary>
            /// The chunk id.
            /// </summary>
            [Rank(2, true)]
            public readonly Guid ChunkID;

            /// <summary>
            /// The chunk size.
            /// </summary>
            [Rank(3)]
            public readonly int ChunkSize;

            /// <summary>
            /// The timestamp.
            /// </summary>
            [Rank(0, true)]
            public readonly DateTime Timestamp;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            ///   Initializes a new instance of the <see cref="PatternA" /> class.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public PatternA()
            {
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternA"/> class.
            /// </summary>
            /// <param name="timestamp">
            /// The timestamp. 
            /// </param>
            /// <param name="accountHrid">
            /// The account hrid. 
            /// </param>
            /// <param name="chunkID">
            /// The chunk ID. 
            /// </param>
            /// <param name="chunkSize">
            /// Size of the chunk. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternA(DateTime timestamp, long accountHrid, Guid chunkID, int chunkSize)
            {
                this.Timestamp = timestamp;
                this.AccountHRID = accountHrid;
                this.ChunkID = chunkID;
                this.ChunkSize = chunkSize;
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
                    return "my-test-container";
                }
            }

            #endregion
        }

        /// <summary>
        /// The pattern b.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternB : BlobName<string>
        {
            // not a field
            #region Constants and Fields

            /// <summary>
            /// The account hrid.
            /// </summary>
            [Rank(0)]
            public readonly long AccountHRID;

            /// <summary>
            /// The chunk id.
            /// </summary>
            [Rank(1)]
            public readonly Guid ChunkID;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternB"/> class.
            /// </summary>
            /// <param name="chunkID">
            /// The chunk ID. 
            /// </param>
            /// <param name="accountHrid">
            /// The account hrid. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternB(Guid chunkID, long accountHrid)
            {
                this.ChunkID = chunkID;
                this.AccountHRID = accountHrid;
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
                    return "my-test-container";
                }
            }

            #endregion
        }

        /// <summary>
        /// The pattern c.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternC : BlobName<string>
        {
            // not a field
            #region Constants and Fields

            /// <summary>
            /// The account id.
            /// </summary>
            [Rank(1)]
            public readonly long AccountId;

            /// <summary>
            /// The chunk id.
            /// </summary>
            [Rank(0)]
            public readonly Guid ChunkID;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternC"/> class.
            /// </summary>
            /// <param name="chunkID">
            /// The chunk ID. 
            /// </param>
            /// <param name="accountId">
            /// The account id. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternC(Guid chunkID, long accountId)
            {
                this.ChunkID = chunkID;
                this.AccountId = accountId;
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
                    return "my-test-container";
                }
            }

            #endregion
        }

        /// <summary>
        /// The pattern d.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternD : PatternC
        {
            // position should always respect inheritance
            #region Constants and Fields

            /// <summary>
            /// The user id.
            /// </summary>
            [Rank(0)]
            public readonly long UserId;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternD"/> class.
            /// </summary>
            /// <param name="chunkID">
            /// The chunk ID. 
            /// </param>
            /// <param name="accountId">
            /// The account id. 
            /// </param>
            /// <param name="userId">
            /// The user id. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternD(Guid chunkID, long accountId, long userId)
                : base(chunkID, accountId)
            {
                this.UserId = userId;
            }

            #endregion
        }

        /// <summary>
        /// The pattern e.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternE : BlobName<string>
        {
            // not a field
            #region Constants and Fields

            /// <summary>
            /// The absolute time.
            /// </summary>
            [Rank(1)]
            public readonly DateTimeOffset AbsoluteTime;

            /// <summary>
            /// The user time.
            /// </summary>
            [Rank(0)]
            public readonly DateTime UserTime;

            #endregion

            // 'dummy' is not used on purpose
            // Provided to make sure 'BlobName' does not rely on specific constructor
            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="PatternE"/> class.
            /// </summary>
            /// <param name="userTime">
            /// The user time. 
            /// </param>
            /// <param name="absoluteTime">
            /// The absolute time. 
            /// </param>
            /// <param name="dummy">
            /// The dummy. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public PatternE(DateTime userTime, DateTimeOffset absoluteTime, string dummy)
            {
                this.UserTime = userTime;
                this.AbsoluteTime = absoluteTime;
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
                    return "my-test-container";
                }
            }

            #endregion
        }

        /// <summary>
        /// The pattern f.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternF : BlobName<string>
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the account HRID.
            /// </summary>
            /// <value> The account HRID. </value>
            /// <remarks>
            /// </remarks>
            [Rank(0)]
            public long AccountHRID { get; set; }

            /// <summary>
            ///   Gets or sets the chunk ID.
            /// </summary>
            /// <value> The chunk ID. </value>
            /// <remarks>
            /// </remarks>
            [Rank(1)]
            public Guid ChunkID { get; set; }

            /// <summary>
            ///   Name of the container where the blob is located.
            /// </summary>
            /// <remarks>
            /// </remarks>
            public override string ContainerName
            {
                get
                {
                    return "my-test-container";
                }
            }

            #endregion
        }

        /// <summary>
        /// The pattern h.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class PatternH : BlobName<string>
        {
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
                    return "my-test-container";
                }
            }

            /// <summary>
            ///   Gets or sets the int id.
            /// </summary>
            /// <value> The int id. </value>
            /// <remarks>
            /// </remarks>
            [Rank(1)]
            public int? IntId { get; set; }

            /// <summary>
            ///   Gets or sets the long id.
            /// </summary>
            /// <value> The long id. </value>
            /// <remarks>
            /// </remarks>
            [Rank(0)]
            public long? LongId { get; set; }

            #endregion
        }
    }
}