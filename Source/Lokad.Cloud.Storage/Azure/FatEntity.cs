#region Copyright (c) Lokad 2010-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Lokad.Cloud.Storage.Tables;

    using Microsoft.WindowsAzure.StorageClient;

    /// <summary>
    /// This entity is basically a workaround the 64KB limitation for entity properties. 15 properties represents a total storage capability of 960KB (entity limit is at 1024KB).
    /// </summary>
    /// <remarks>
    /// This class is basically a hack against the Table Storage to work-around the 64KB limitation for properties.
    /// </remarks>
    public class FatEntity : TableServiceEntity
    {
        #region Constants and Fields

        /// <summary>
        ///   Maximal entity size is 1MB. Out of that, we keep only 960kb (1MB - 64kb as a safety margin). Then, it should be taken into account that byte[] are Base64 encoded which represent a penalty overhead of 4/3 - hence the reduced capacity.
        /// </summary>
        public const int MaxByteCapacity = (960 * 1024 * 3) / 4;

        #endregion

        // ReSharper disable InconsistentNaming
        // ReSharper disable MemberCanBePrivate.Global
        #region Public Properties

        /// <summary>
        ///   Gets or sets the p0.
        /// </summary>
        /// <value> The p0. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P0 { get; set; }

        /// <summary>
        ///   Gets or sets the p1.
        /// </summary>
        /// <value> The p1. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P1 { get; set; }

        /// <summary>
        ///   Gets or sets the P10.
        /// </summary>
        /// <value> The P10. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P10 { get; set; }

        /// <summary>
        ///   Gets or sets the P11.
        /// </summary>
        /// <value> The P11. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P11 { get; set; }

        /// <summary>
        ///   Gets or sets the P12.
        /// </summary>
        /// <value> The P12. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P12 { get; set; }

        /// <summary>
        ///   Gets or sets the P13.
        /// </summary>
        /// <value> The P13. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P13 { get; set; }

        /// <summary>
        ///   Gets or sets the P14.
        /// </summary>
        /// <value> The P14. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P14 { get; set; }

        /// <summary>
        ///   Gets or sets the p2.
        /// </summary>
        /// <value> The p2. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P2 { get; set; }

        /// <summary>
        ///   Gets or sets the p3.
        /// </summary>
        /// <value> The p3. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P3 { get; set; }

        /// <summary>
        ///   Gets or sets the p4.
        /// </summary>
        /// <value> The p4. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P4 { get; set; }

        /// <summary>
        ///   Gets or sets the p5.
        /// </summary>
        /// <value> The p5. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P5 { get; set; }

        /// <summary>
        ///   Gets or sets the p6.
        /// </summary>
        /// <value> The p6. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P6 { get; set; }

        /// <summary>
        ///   Gets or sets the p7.
        /// </summary>
        /// <value> The p7. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P7 { get; set; }

        /// <summary>
        ///   Gets or sets the p8.
        /// </summary>
        /// <value> The p8. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P8 { get; set; }

        /// <summary>
        ///   Gets or sets the p9.
        /// </summary>
        /// <value> The p9. </value>
        /// <remarks>
        /// </remarks>
        public byte[] P9 { get; set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Converts a <c>FatEntity</c> toward a <c>CloudEntity</c> .
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="fatEntity">
        /// The fat entity. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="etag">
        /// The etag. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static CloudEntity<T> Convert<T>(FatEntity fatEntity, IDataSerializer serializer, string etag)
        {
            using (var stream = new MemoryStream(fatEntity.GetData()) { Position = 0 })
            {
                var val = (T)serializer.Deserialize(stream, typeof(T));

                return new CloudEntity<T>
                    {
                        PartitionKey = fatEntity.PartitionKey,
                        RowKey = fatEntity.RowKey,
                        Timestamp = fatEntity.Timestamp,
                        ETag = etag,
                        Value = val
                    };
            }
        }

        /// <summary>
        /// Converts a <c>CloudEntity</c> toward a <c>FatEntity</c> .
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="cloudEntity">
        /// The cloud entity. 
        /// </param>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static FatEntity Convert<T>(CloudEntity<T> cloudEntity, IDataSerializer serializer)
        {
            var fatEntity = new FatEntity
                {
                    PartitionKey = cloudEntity.PartitionKey, 
                    RowKey = cloudEntity.RowKey, 
                    Timestamp = cloudEntity.Timestamp
                };

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(cloudEntity.Value, stream, typeof(T));
                fatEntity.SetData(stream.ToArray());
                return fatEntity;
            }
        }

        /// <summary>
        /// Returns the concatenated stream contained in the fat entity.
        /// </summary>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public byte[] GetData()
        {
            var arrays = this.GetProperties().ToArray();
            var buffer = new byte[arrays.Sum(a => a.Length)];

            var i = 0;
            foreach (var array in arrays)
            {
                Buffer.BlockCopy(array, 0, buffer, i, array.Length);
                i += array.Length;
            }

            return buffer;
        }

        /// <summary>
        /// Returns an upper bound approximation of the payload associated to the entity once serialized as XML Atom (used for communication with the Table Storage).
        /// </summary>
        /// <returns>
        /// The get payload.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public int GetPayload()
        {
            // measurements indicates overhead is closer to 1300 chars, but we take a bit of margin
            const int envelopOverhead = 1500;

            // Caution: there is a loss when converting byte[] to Base64 representation
            var binCharCount = (this.GetProperties().Sum(a => a.Length) * 4 + 3) / 3;
            var partitionKeyCount = this.PartitionKey.Length;
            var rowKeyCount = this.RowKey.Length;

            // timestamp is already accounted for in the envelop overhead.
            return binCharCount + partitionKeyCount + rowKeyCount + envelopOverhead;
        }

        /// <summary>
        /// Split the stream as a fat entity.
        /// </summary>
        /// <param name="data">
        /// The data. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public void SetData(byte[] data)
        {
            if (null == data)
            {
                throw new ArgumentNullException("data");
            }

            if (data.Length >= MaxByteCapacity)
            {
                throw new ArgumentOutOfRangeException("data");
            }

            var setters = new Action<byte[]>[]
                {
                    b => this.P0 = b, b => this.P1 = b, b => this.P2 = b, b => this.P3 = b, b => this.P4 = b, 
                    b => this.P5 = b, b => this.P6 = b, b => this.P7 = b, b => this.P8 = b, b => this.P9 = b, 
                    b => this.P10 = b, b => this.P11 = b, b => this.P12 = b, b => this.P13 = b, b => this.P14 = b
                };

            for (var i = 0; i < 15; i++)
            {
                if (i * 64 * 1024 < data.Length)
                {
                    var start = i * 64 * 1024;
                    var length = Math.Min(64 * 1024, data.Length - start);
                    var buffer = new byte[length];

                    Buffer.BlockCopy(data, start, buffer, 0, buffer.Length);
                    setters[i](buffer);
                }
                else
                {
                    setters[i](null); // discarding potential leftover
                }
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Gets the properties.
        /// </summary>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        private IEnumerable<byte[]> GetProperties()
        {
            if (null != this.P0)
            {
                yield return this.P0;
            }

            if (null != this.P1)
            {
                yield return this.P1;
            }

            if (null != this.P2)
            {
                yield return this.P2;
            }

            if (null != this.P3)
            {
                yield return this.P3;
            }

            if (null != this.P4)
            {
                yield return this.P4;
            }

            if (null != this.P5)
            {
                yield return this.P5;
            }

            if (null != this.P6)
            {
                yield return this.P6;
            }

            if (null != this.P7)
            {
                yield return this.P7;
            }

            if (null != this.P8)
            {
                yield return this.P8;
            }

            if (null != this.P9)
            {
                yield return this.P9;
            }

            if (null != this.P10)
            {
                yield return this.P10;
            }

            if (null != this.P11)
            {
                yield return this.P11;
            }

            if (null != this.P12)
            {
                yield return this.P12;
            }

            if (null != this.P13)
            {
                yield return this.P13;
            }

            if (null != this.P14)
            {
                yield return this.P14;
            }
        }

        #endregion
    }
}