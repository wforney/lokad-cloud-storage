#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Tables
{
    using System;
    using System.Runtime.Serialization;

    using Lokad.Cloud.Storage.Azure;
    using Lokad.Cloud.Storage.Tables;

    using NUnit.Framework;

    /// <summary>
    /// The fat entity tests.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [TestFixture]
    public class FatEntityTests
    {
        #region Constants and Fields

        /// <summary>
        /// The serializer.
        /// </summary>
        private readonly IDataSerializer serializer = new CloudFormatter();

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Converts this instance.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void Convert()
        {
            var timevalues = new TimeValue[20000];
            for (var i = 0; i < timevalues.Length; i++)
            {
                timevalues[i] = new TimeValue { Time = new DateTime(2001, 1, 1).AddMinutes(i), Value = i };
            }

            var serie = new TimeSerie { TimeValues = timevalues };

            var cloudEntity = new CloudEntity<TimeSerie> { PartitionKey = "part", RowKey = "key", Value = serie };

            var fatEntity = FatEntity.Convert(cloudEntity, this.serializer);
            var cloudEntity2 = FatEntity.Convert<TimeSerie>(fatEntity, this.serializer, null);
            var fatEntity2 = FatEntity.Convert(cloudEntity2, this.serializer);

            Assert.IsNotNull(cloudEntity2);
            Assert.IsNotNull(fatEntity2);

            Assert.AreEqual(cloudEntity.PartitionKey, fatEntity.PartitionKey);
            Assert.AreEqual(cloudEntity.RowKey, fatEntity.RowKey);

            Assert.AreEqual(cloudEntity.PartitionKey, fatEntity2.PartitionKey);
            Assert.AreEqual(cloudEntity.RowKey, fatEntity2.RowKey);

            Assert.IsNotNull(cloudEntity2.Value);
            Assert.AreEqual(cloudEntity.Value.TimeValues.Length, cloudEntity2.Value.TimeValues.Length);

            for (var i = 0; i < timevalues.Length; i++)
            {
                Assert.AreEqual(cloudEntity.Value.TimeValues[i].Time, cloudEntity2.Value.TimeValues[i].Time);
                Assert.AreEqual(cloudEntity.Value.TimeValues[i].Value, cloudEntity2.Value.TimeValues[i].Value);
            }

            var data1 = fatEntity.GetData();
            var data2 = fatEntity2.GetData();
            Assert.AreEqual(data1.Length, data2.Length);
            for (var i = 0; i < data2.Length; i++)
            {
                Assert.AreEqual(data1[i], data2[i]);
            }
        }

        /// <summary>
        /// Converts the no contract.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void ConvertNoContract()
        {
            var timevalues = new TimeValueNoContract[20000];
            for (var i = 0; i < timevalues.Length; i++)
            {
                timevalues[i] = new TimeValueNoContract { Time = new DateTime(2001, 1, 1).AddMinutes(i), Value = i };
            }

            var serie = new TimeSerieNoContract { TimeValues = timevalues };

            var cloudEntity = new CloudEntity<TimeSerieNoContract> { PartitionKey = "part", RowKey = "key", Value = serie };

            var fatEntity = FatEntity.Convert(cloudEntity, this.serializer);
            var cloudEntity2 = FatEntity.Convert<TimeSerieNoContract>(fatEntity, this.serializer, null);
            var fatEntity2 = FatEntity.Convert(cloudEntity2, this.serializer);

            Assert.IsNotNull(cloudEntity2);
            Assert.IsNotNull(fatEntity2);

            Assert.AreEqual(cloudEntity.PartitionKey, fatEntity.PartitionKey);
            Assert.AreEqual(cloudEntity.RowKey, fatEntity.RowKey);

            Assert.AreEqual(cloudEntity.PartitionKey, fatEntity2.PartitionKey);
            Assert.AreEqual(cloudEntity.RowKey, fatEntity2.RowKey);

            Assert.IsNotNull(cloudEntity2.Value);
            Assert.AreEqual(cloudEntity.Value.TimeValues.Length, cloudEntity2.Value.TimeValues.Length);

            for (var i = 0; i < timevalues.Length; i++)
            {
                Assert.AreEqual(cloudEntity.Value.TimeValues[i].Time, cloudEntity2.Value.TimeValues[i].Time);
                Assert.AreEqual(cloudEntity.Value.TimeValues[i].Value, cloudEntity2.Value.TimeValues[i].Value);
            }

            var data1 = fatEntity.GetData();
            var data2 = fatEntity2.GetData();
            Assert.AreEqual(data1.Length, data2.Length);
            for (var i = 0; i < data2.Length; i++)
            {
                Assert.AreEqual(data1[i], data2[i]);
            }
        }

        #endregion

        /// <summary>
        /// The time serie.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [DataContract]
        public class TimeSerie
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the time values.
            /// </summary>
            /// <value> The time values. </value>
            /// <remarks>
            /// </remarks>
            [DataMember]
            public TimeValue[] TimeValues { get; set; }

            #endregion
        }

        /// <summary>
        /// The time serie no contract.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Serializable]
        public class TimeSerieNoContract
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the time values.
            /// </summary>
            /// <value> The time values. </value>
            /// <remarks>
            /// </remarks>
            public TimeValueNoContract[] TimeValues { get; set; }

            #endregion
        }

        /// <summary>
        /// The time value.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [DataContract]
        public class TimeValue
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the time.
            /// </summary>
            /// <value> The time. </value>
            /// <remarks>
            /// </remarks>
            [DataMember]
            public DateTime Time { get; set; }

            /// <summary>
            ///   Gets or sets the value.
            /// </summary>
            /// <value> The value. </value>
            /// <remarks>
            /// </remarks>
            [DataMember]
            public double Value { get; set; }

            #endregion
        }

        /// <summary>
        /// The time value no contract.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Serializable]
        public class TimeValueNoContract
        {
            #region Public Properties

            /// <summary>
            ///   Gets or sets the time.
            /// </summary>
            /// <value> The time. </value>
            /// <remarks>
            /// </remarks>
            public DateTime Time { get; set; }

            /// <summary>
            ///   Gets or sets the value.
            /// </summary>
            /// <value> The value. </value>
            /// <remarks>
            /// </remarks>
            public double Value { get; set; }

            #endregion
        }
    }
}