﻿#region Copyright (c) Lokad 2009-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Lokad.Cloud.Storage.Test.Shared;
using NUnit.Framework;

namespace Lokad.Cloud.Storage.Test.Tables
{
    using Lokad.Cloud.Storage.Tables;

    [TestFixture]
    public abstract class TableStorageTests
    {
        readonly static Random Rand = new Random();

        protected readonly ITableStorageProvider TableStorage;

        const string TableName = "teststablestorageprovidermytable";

        public TableStorageTests()
            : this(CloudStorage.ForDevelopmentStorage().BuildStorageProviders())
        {
        }

        protected TableStorageTests(CloudStorageProviders storage)
        {
            TableStorage = storage.TableStorage;
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            TableStorage.DeleteTable(TableName);
        }

        [Test]
        public void CreateDeleteTables()
        {
            var name = "n" + Guid.NewGuid().ToString("N");
            Assert.IsTrue(TableStorage.CreateTable(name), "#A01");
            Assert.IsFalse(TableStorage.CreateTable(name), "#A02");
            Assert.IsTrue(TableStorage.DeleteTable(name), "#A03");

            // replicating the test a 2nd time, to check for slow table deletion
            Assert.IsTrue(TableStorage.CreateTable(name), "#A04");
            Assert.IsTrue(TableStorage.DeleteTable(name), "#A05");

            Assert.IsFalse(TableStorage.DeleteTable(name), "#A06");

            const string name2 = "IamNotATable";
            Assert.IsFalse(TableStorage.DeleteTable(name2), "#A07");
        }

        [Test]
        public void GetTables()
        {
            TableStorage.CreateTable(TableName);
            var tables = TableStorage.GetTables();
            Assert.IsTrue(tables.Contains(TableName), "#B07");
        }

        [Test]
        public void GetOnMissingTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");

            // checking the 4 overloads
            var enumerable = TableStorage.Get<string>(missingTableName);
            int count = enumerable.Count();
            Assert.AreEqual(0, count, "#A00");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition");
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A01");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition", "start", "end");
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A02");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition", new[] { "my-key" });
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A03");
        }

        [Test]
        public void GetOnJustDeletedTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            Assert.IsTrue(TableStorage.CreateTable(missingTableName), "#A01");
            Assert.IsTrue(TableStorage.DeleteTable(missingTableName), "#A02");

            // checking the 4 overloads
            var enumerable = TableStorage.Get<string>(missingTableName);
            int count = enumerable.Count();
            Assert.AreEqual(0, count, "#A00");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition");
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A01");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition", "start", "end");
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A02");

            enumerable = TableStorage.Get<string>(missingTableName, "my-partition", new[] { "my-key" });
            count = enumerable.Count();
            Assert.AreEqual(0, count, "#A03");
        }

        [Test]
        public void GetOnMissingPartitionShouldWork()
        {
            var missingPartition = Guid.NewGuid().ToString("N");

            var enumerable = TableStorage.Get<string>(TableName, missingPartition);
            Assert.That(!enumerable.Any(), "#D01");

            var enumerable2 = TableStorage.Get<string>(TableName, missingPartition, "dummyRowKeyA", "dummyRowKeyB");
            Assert.That(!enumerable2.Any(), "#D02");

            var enumerable3 = TableStorage.Get<string>(TableName, missingPartition, new[] { "dummyRowKeyA", "dummyRowKeyB" });
            Assert.That(!enumerable3.Any(), "#D02");
        }

        [Test]
        public void InsertOnMissingTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            TableStorage.Insert(missingTableName, Entities(1, "my-key", 10));

            // tentative clean-up
            TableStorage.DeleteTable(missingTableName);
        }

        [Test]
        public void InsertOnJustDeletedTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            Assert.IsTrue(TableStorage.CreateTable(missingTableName), "#A01");
            Assert.IsTrue(TableStorage.DeleteTable(missingTableName), "#A02");
            TableStorage.Insert(missingTableName, Entities(1, "my-key", 10));

            // tentative clean-up
            TableStorage.DeleteTable(missingTableName);
        }

        [Test]
        public void UpsertOnMissingTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            TableStorage.Upsert(missingTableName, Entities(1, "my-key", 10));

            // tentative clean-up
            TableStorage.DeleteTable(missingTableName);
        }

        [Test]
        public void UpsertShouldSupportLargeEntityCount()
        {
            const string p1 = "00049999DatasetRepositoryTests";

            var entities = Entities(150, p1, 10);
            for (int i = 0; i < entities.Length; i++ )
            {
                entities[i].RowKey = "series+" + i;	
            }

            TableStorage.Upsert(TableName, entities);
            TableStorage.Upsert(TableName, entities); // idempotence

            var list = TableStorage.Get<string>(TableName, p1).ToArray();
            Assert.AreEqual(entities.Length, list.Length, "#A00");
        }

        [Test]
        public void UpsertShouldUpdateOrInsert()
        {
            var p1 = Guid.NewGuid().ToString("N");
            var p2 = Guid.NewGuid().ToString("N");

            var e1 = Entities(15, p1, 10);
            var e2 = Entities(25, p2, 10);
            var e1And2 = e1.Union(e2).ToArray();

            TableStorage.Upsert(TableName, e1);
            TableStorage.Upsert(TableName, e1And2);

            var count1 = TableStorage.Get<string>(TableName, p1).Count();
            Assert.AreEqual(e1.Length, count1, "#A00");

            var count2 = TableStorage.Get<string>(TableName, p2).Count();
            Assert.AreEqual(e2.Length, count2, "#A01");
        }

        [Test]
        public void InsertShouldHandleDistinctPartition()
        {
            var p1 = Guid.NewGuid().ToString("N");
            var p2 = Guid.NewGuid().ToString("N");

            var e1 = Entities(15, p1, 10);
            var e2 = Entities(25, p2, 10);
            var e1And2 = e1.Union(e2);

            TableStorage.Insert(TableName, e1And2);
        }

        [Test]
        public void GetWithPartitionShouldOnlySpecifiedPartition()
        {
            var p1 = Guid.NewGuid().ToString("N");
            var p2 = Guid.NewGuid().ToString("N");

            var e1 = Entities(15, p1, 10);
            var e2 = Entities(25, p2, 10);
            var e1And2 = e1.Union(e2);

            TableStorage.Insert(TableName, e1And2);

            var list1 = TableStorage.Get<string>(TableName, p1).ToArray();
            var count1 = list1.Length;
            Assert.AreEqual(e1.Length, count1, "#A00");
        }

        [Test]
        public void DeleteOnMissingTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            TableStorage.Delete<string>(missingTableName, "my-part", new[] { "my-key" });
        }

        [Test]
        public void DeleteOnJustDeletedTableShouldWork()
        {
            var missingTableName = "t" + Guid.NewGuid().ToString("N");
            Assert.IsTrue(TableStorage.CreateTable(missingTableName), "#A01");
            Assert.IsTrue(TableStorage.DeleteTable(missingTableName), "#A02");
            TableStorage.Delete<string>(missingTableName, "my-part", new[] { "my-key" });
        }

        [Test]
        public void DeleteOnMissingPartitionShouldWork()
        {
            var missingPartition = Guid.NewGuid().ToString("N");
            TableStorage.Delete<string>(TableName, missingPartition, new[] { "my-key" });
        }

        [Test]
        public void UpdateFailsOnMissingTable()
        {
            try
            {
                var missingTableName = "t" + Guid.NewGuid().ToString("N");
                TableStorage.Update(missingTableName, Entities(1, "my-key", 10));
                Assert.Fail("#A00");
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Test]
        public void UpdateFailsOnJustDeletedTable()
        {
            try
            {
                var missingTableName = "t" + Guid.NewGuid().ToString("N");
                Assert.IsTrue(TableStorage.CreateTable(missingTableName), "#A01");
                Assert.IsTrue(TableStorage.DeleteTable(missingTableName), "#A02");
                TableStorage.Update(missingTableName, Entities(1, "my-key", 10));
                Assert.Fail("#A00");
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Test]
        public void UpdateFailsOnMissingPartition()
        {
            try
            {
                var missingPartition = Guid.NewGuid().ToString("N");
                TableStorage.Update(TableName, Entities(1, missingPartition, 10));
                Assert.Fail("#A00");
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Test]
        public void GetMethodStartEnd()
        {
            //This is a test on the ordered enumeration return by the GetMethod with StartRowKEy-EndRowKey.
            const int N = 250;
            string pKey = Guid.NewGuid().ToString();

            TableStorage.CreateTable(TableName);
            var entities = Enumerable.Range(0, N).Select(i => new CloudEntity<string>
                    {
                        PartitionKey = pKey,
                        RowKey = "RowKey" + i,
                        Value = Guid.NewGuid().ToString()
                    });

            TableStorage.Insert(TableName, entities);

            var retrieved = TableStorage.Get<string>(TableName, pKey, null, null).ToArray();
            var retrievedSorted = retrieved.OrderBy(e => e.RowKey).ToArray();

            bool isOrdered = true;
            for (int i = 0; i < retrieved.Length; i++)
            {
                if (retrieved[i] != retrievedSorted[i])
                {
                    isOrdered = false;
                    break;
                }
            }
            Assert.That(isOrdered, "#C01");

            var retrieved2 = TableStorage.Get<string>(TableName, pKey, "RowKey25", null).ToArray();
            var retrievedSorted2 = retrieved2.OrderBy(e => e.RowKey).ToArray();

            bool isOrdered2 = true;
            for (int i = 0; i < retrieved2.Length; i++)
            {
                if (retrieved2[i] != retrievedSorted2[i])
                {
                    isOrdered2 = false;
                    break;
                }
            }
            Assert.That(isOrdered2, "#C02");

            var retrieved3 = TableStorage.Get<string>(TableName, pKey, null, "RowKey25").ToArray();
            var retrievedSorted3 = retrieved3.OrderBy(e => e.RowKey).ToArray();

            bool isOrdered3 = true;
            for (int i = 0; i < retrieved3.Length; i++)
            {
                if (retrieved3[i] != retrievedSorted3[i])
                {
                    isOrdered3 = false;
                    break;
                }
            }
            Assert.That(isOrdered3, "#C03");
        }

        [Test]
        public void InsertAndUpdateFailures()
        {
            var partitionKey = Guid.NewGuid().ToString();
            var rowKey = Guid.NewGuid().ToString();

            var entity = new CloudEntity<string>
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    Timestamp = DateTime.UtcNow,
                    Value = "value1"
                };

            // Insert entity.
            TableStorage.Insert(TableName, new[] { entity });

            // Insert Retry should fail.
            try
            {
                TableStorage.Insert(TableName, new[] { entity });
                Assert.Fail("#A01");
            }
            catch (InvalidOperationException)
            {
            }

            // Update entity twice should fail
            try
            {
                entity.Value = "value2";
                TableStorage.Update(TableName, new[] { entity, entity });
                Assert.Fail("#A02");
            }
            catch (InvalidOperationException)
            {
            }

            // Delete entity.
            TableStorage.Delete<string>(TableName, partitionKey, new[] { rowKey });

            // Update deleted entity should fail
            try
            {
                entity.Value = "value2";
                TableStorage.Update(TableName, new[] { entity });
                Assert.Fail("#A03");
            }
            catch (InvalidOperationException)
            {
            }

            // Insert entity twice should fail
            try
            {
                TableStorage.Insert(TableName, new[] { entity, entity });
                Assert.Fail("#A04");
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Test]
        public void IdempotenceOfDeleteMethod()
        {
            var pkey = Guid.NewGuid().ToString("N");

            var entities = Range.Array(10).Select(i =>
                new CloudEntity<string>
                    {
                        PartitionKey = pkey,
                        RowKey = Guid.NewGuid().ToString("N"),
                        Value = "nothing"
                    }).ToArray();

            // Insert/delete entity.
            TableStorage.Insert(TableName, entities);

            // partial deletion
            TableStorage.Delete<string>(TableName, pkey, entities.Take(5).Select(e => e.RowKey));

            // complete deletion, but with overlap
            TableStorage.Delete<string>(TableName, pkey, entities.Select(e => e.RowKey));

            // checking that all entities have been deleted
            var list = TableStorage.Get<string>(TableName, pkey, entities.Select(e => e.RowKey));
            Assert.That(!list.Any(), "#A00");
        }

        [Test]
        public void CheckInsertHandlingOfEntityMaxCount()
        {
            // above the max entity count limit
            const int entityCount = 300;
            var partitionKey = Guid.NewGuid().ToString();

            TableStorage.Insert(TableName, Entities(entityCount, partitionKey, 1));
            var retrievedCount = TableStorage.Get<string>(TableName, partitionKey).Count();

            Assert.AreEqual(entityCount, retrievedCount);
        }

        [Test]
        public void CheckRangeSelection()
        {
            // above the max entity count limit
            const int entityCount = 300;
            var partitionKey = Guid.NewGuid().ToString();

            // entities are sorted
            var entities = Entities(entityCount, partitionKey, 1).OrderBy(e => e.RowKey).ToArray();

            TableStorage.Insert(TableName, entities);

            var retrievedCount = TableStorage.Get<string>(TableName, partitionKey,
                entities[150].RowKey, entities[200].RowKey).Count();

            // only the range should have been retrieved
            Assert.AreEqual(200 - 150, retrievedCount);
        }

        [Test]
        public void CheckInsertHandlingOfHeavyTransaction()
        {
            const int entityCount = 50;
            var partitionKey = Guid.NewGuid().ToString();

            // 5 MB is above the max entity transaction payload
            TableStorage.Insert(TableName, Entities(entityCount, partitionKey, 100 * 1024));
            var retrievedCount = TableStorage.Get<string>(TableName, partitionKey).Count();

            Assert.AreEqual(entityCount, retrievedCount);
        }

        [Test]
        public void ErrorCodeExtraction()
        {
            // HACK: just reproducing the code being tested, no direct linking
            var r = new Regex(@"<code>(\w+)</code>", RegexOptions.IgnoreCase);

            var errorMessage =
@"<?xml version=""1.0"" encoding=""utf-8"" standalone=""yes""?>
<error xmlns=""http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"">
  <code>OperationTimedOut</code>
  <message xml:lang=""en-US"">Operation could not be completed within the specified time.
RequestId:f8e1e934-99ca-4a6f-bca7-e8e5fbd059ea
Time:2010-01-15T12:37:25.1611631Z</message>
</error>";

            var ex = new DataServiceRequestException("", new Exception(errorMessage));

            Assert.AreEqual("OperationTimedOut", GetErrorCode(ex));

        }

        static string GetErrorCode(DataServiceRequestException ex)
        {
            var r = new Regex(@"<code>(\w+)</code>", RegexOptions.IgnoreCase);
            var match = r.Match(ex.InnerException.Message);
            return match.Groups[1].Value;
        }

        [Test]
        public void EntityKeysShouldSupportSpecialCharacters()
        {
            // disallowed: /\#? must be <1 KB; we also disallow ' for simplicity
            var keys = new[] {"abc", "123", "abc-123", "abc def", "abc@def", "*", "+", "~%_;:.,"};

            var entities = keys.Select(key => new CloudEntity<string>
                {
                    PartitionKey = key,
                    RowKey = key,
                    Value = key,
                }).ToArray();

            TableStorage.Insert(TableName, entities);

            var result = keys.Select(key => TableStorage.Get<string>(TableName, key, key).Value).ToArray();
            CollectionAssert.AreEqual(keys, result.Select(e => e.Value));
            CollectionAssert.AreEqual(keys, result.Select(e => e.PartitionKey));
            CollectionAssert.AreEqual(keys, result.Select(e => e.RowKey));

            foreach (var key in keys)
            {
                TableStorage.Delete<string>(TableName, key, new[] {key});
            }
        }

        [Test]
        public void EntitiesShouldHaveETagAfterInsert()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(3, partition, 10);

            TableStorage.Insert(TableName, entities);

            // note: ETags are not unique (they're actually the same per request)
            CollectionAssert.AllItemsAreNotNull(entities.Select(e => e.ETag));
        }

        [Test]
        public void EntitiesShouldHaveNewETagAfterUpdate()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(3, partition, 10);

            TableStorage.Insert(TableName, entities);

            var oldETags = entities.Select(e => e.ETag).ToArray();

            foreach(var entity in entities)
            {
                entity.Value += "modified";
            }

            TableStorage.Update(TableName, entities);

            var newETags = entities.Select(e => e.ETag).ToArray();
            CollectionAssert.AllItemsAreNotNull(newETags);
            CollectionAssert.AreNotEqual(newETags, oldETags);
        }

        [Test, ExpectedException(typeof(DataServiceRequestException))]
        public void UpdateOnRemotelyModifiedEntityShouldFailIfNotForced()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = "abc";
            entity.Value = "def";

            TableStorage.Update(TableName, entities, false);
        }

        [Test]
        public void UpdateOnRemotelyModifiedEntityShouldNotFailIfForced()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = "abc";
            entity.Value = "def";

            TableStorage.Update(TableName, entities, true);
        }

        [Test]
        public void UpdateOnRemotelyModifiedEntityShouldNotFailIfETagIsNull()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = null;
            entity.Value = "def";

            TableStorage.Update(TableName, entities, false);
        }

        [Test, ExpectedException(typeof(DataServiceRequestException))]
        public void DeleteOnRemotelyModifiedEntityShouldFailIfNotForced()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = "abc";
            entity.Value = "def";

            TableStorage.Delete(TableName, entities, false);
        }

        [Test]
        public void DeleteOnRemotelyModifiedEntityShouldNotFailIfForced()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = "abc";
            entity.Value = "def";

            TableStorage.Delete(TableName, entities, true);
        }

        [Test]
        public void DeleteOnRemotelyModifiedEntityShouldNotFailIfETagIsNull()
        {
            var partition = Guid.NewGuid().ToString("N");
            var entities = Entities(1, partition, 10);
            var entity = entities.First();

            TableStorage.Insert(TableName, entities);

            entity.ETag = null;
            entity.Value = "def";

            TableStorage.Delete(TableName, entities, false);
        }

        CloudEntity<String>[] Entities(int count, string partitionKey, int entitySize)
        {
            return EntitiesInternal(count, partitionKey, entitySize).ToArray();
        }

        IEnumerable<CloudEntity<String>> EntitiesInternal(int count, string partitionKey, int entitySize)
        {
            for (int i = 0; i < count; i++)
            {
                yield return new CloudEntity<string>
                    {
                        PartitionKey = partitionKey,
                        RowKey = Guid.NewGuid().ToString(),
                        Value = RandomString(entitySize)
                    };
            }
        }

        public static string RandomString(int size)
        {
            var builder = new StringBuilder();
            for (int i = 0; i < size; i++)
            {

                //26 letters in the alfabet, ascii + 65 for the capital letters
                builder.Append(Convert.ToChar(Convert.ToInt32(Rand.Next(26) + 65)));

            }
            return builder.ToString();
        }
    }
}
