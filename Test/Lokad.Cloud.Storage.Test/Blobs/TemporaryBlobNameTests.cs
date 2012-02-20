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
    /// The temporary blob name tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public class TemporaryBlobNameTests
    {
        #region Public Methods and Operators

        /// <summary>
        /// Specializeds the temporary BLOB names can be parsed as base class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void SpecializedTemporaryBlobNamesCanBeParsedAsBaseClass()
        {
            var now = DateTimeOffset.UtcNow;

            // round to seconds, our time resolution in blob names
            now = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Offset);

            var testRef = new TestTemporaryBlobName(now, "test", Guid.NewGuid());
            var printed = UntypedBlobName.Print(testRef);

            var parsedRef = UntypedBlobName.Parse<TemporaryBlobName<object>>(printed);
            Assert.AreEqual(now, parsedRef.Expiration);
            Assert.AreEqual("test", parsedRef.Suffix);
        }

        /// <summary>
        /// Temporaries the BLOB references are unique.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void TemporaryBlobReferencesAreUnique()
        {
            var expiration = new DateTime(2100, 12, 31);
            var firstBlobRef = TemporaryBlobName<int>.GetNew(expiration);
            var secondBlobRef = TemporaryBlobName<int>.GetNew(expiration);

            Assert.AreNotEqual(
                firstBlobRef.Suffix, 
                secondBlobRef.Suffix, 
                "two different temporary blob references should have different prefix");
        }

        #endregion

        /// <summary>
        /// The test temporary blob name.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class TestTemporaryBlobName : TemporaryBlobName<int>
        {
            #region Constants and Fields

            /// <summary>
            ///   The id.
            /// </summary>
            [Rank(0)]
            public readonly Guid Id;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            /// Initializes a new instance of the <see cref="TestTemporaryBlobName"/> class.
            /// </summary>
            /// <param name="expiration">
            /// The expiration. 
            /// </param>
            /// <param name="prefix">
            /// The prefix. 
            /// </param>
            /// <param name="id">
            /// The id. 
            /// </param>
            /// <remarks>
            /// </remarks>
            public TestTemporaryBlobName(DateTimeOffset expiration, string prefix, Guid id)
                : base(expiration, prefix)
            {
                this.Id = id;
            }

            #endregion
        }
    }
}