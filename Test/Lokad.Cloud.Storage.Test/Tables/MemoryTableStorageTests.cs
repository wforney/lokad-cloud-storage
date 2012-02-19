#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Test.Tables
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;

    using Lokad.Cloud.Storage.Tables;

    using NUnit.Framework;

    /// <summary>
    /// The memory table storage tests.
    /// </summary>
    /// <remarks>
    /// Includes all unit tests for the real table provider
    /// </remarks>
    [TestFixture]
    [Category("InMemoryStorage")]
    public class MemoryTableStorageTests : TableStorageTests
    {
        #region Constructors and Destructors

        /// <summary>
        ///   Initializes a new instance of the <see cref="MemoryTableStorageTests" /> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public MemoryTableStorageTests()
            : base(CloudStorage.ForInMemoryStorage().BuildStorageProviders())
        {
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Creates the and get table.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CreateAndGetTable()
        {
            var originalCount = this.TableStorage.GetTables().Count();

            // Single thread.
            for (var i = 0; i <= 5; i++)
            {
                this.TableStorage.CreateTable(string.Format("table{0}", i.ToString(CultureInfo.InvariantCulture)));
            }

            Assert.AreEqual(6, this.TableStorage.GetTables().Count() - originalCount, "#A01");

            // Remove tables.
            Assert.False(this.TableStorage.DeleteTable("Table_that_does_not_exist"), "#A02");
            var isSuccess = this.TableStorage.DeleteTable(string.Format("table{0}", 4.ToString(CultureInfo.InvariantCulture)));

            Assert.IsTrue(isSuccess, "#A03");
            Assert.AreEqual(5, this.TableStorage.GetTables().Count() - originalCount, "#A04");
        }

        /// <summary>
        /// Creates the and get tables multiple tasks.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void CreateAndGetTablesMultipleTasks()
        {
            // Multi thread.
            const int M = 32;

            Task.WaitAll(
                Enumerable.Range(0, M).Select(
                    i => Task.Factory.StartNew(
                        () =>
                            {
                                for (var k1 = 0; k1 < 10; k1++)
                                {
                                    this.TableStorage.CreateTable(string.Format("table{0}", k1.ToString(CultureInfo.InvariantCulture)));
                                }
                            })).ToArray());

            Assert.AreEqual(10, this.TableStorage.GetTables().Distinct().Count());
        }

        /// <summary>
        /// Inserts the and get method single thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void InsertAndGetMethodSingleThread()
        {
            const string tableName = "myTable";

            this.TableStorage.CreateTable(tableName);

            const int PartitionCount = 10;

            // Creating entities: a hundred. Pkey created with the last digit of a number between 0 and 99.
            var entities =
                Enumerable.Range(0, 100).Select(
                    i =>
                    new CloudEntity<object>
                        {
                            PartitionKey = string.Format("Pkey-{0}", (i % PartitionCount).ToString("0")),
                            RowKey = string.Format("RowKey-{0}", i.ToString("00")),
                            Value = new object()
                        });

            // Insert entities.
            this.TableStorage.Insert(tableName, entities);

            // retrieve all of them.
            var retrievedEntities1 = this.TableStorage.Get<object>(tableName);
            Assert.AreEqual(100, retrievedEntities1.Count(), "#B01");

            // Test overloads...
            var retrievedEntites2 = this.TableStorage.Get<object>(tableName, "Pkey-9");
            Assert.AreEqual(10, retrievedEntites2.Count(), "#B02");

            var retrievedEntities3 = this.TableStorage.Get<object>(
                tableName, "Pkey-7", new[] { "RowKey-27", "RowKey-37", "IAmNotAKey" });

            Assert.AreEqual(2, retrievedEntities3.Count(), "#B03");

            // The following tests handle the exclusive and inclusive bounds of key search.
            var retrieved4 = this.TableStorage.Get<object>(tableName, "Pkey-1", "RowKey-01", "RowKey-91");
            Assert.AreEqual(9, retrieved4.Count(), "#B04");

            var retrieved5 = this.TableStorage.Get<object>(tableName, "Pkey-1", "RowKey-01", null);
            Assert.AreEqual(10, retrieved5.Count(), "#B05");

            var retrieved6 = this.TableStorage.Get<object>(tableName, "Pkey-1", null, null);
            Assert.AreEqual(10, retrieved6.Count(), "#B06");

            var retrieved7 = this.TableStorage.Get<object>(tableName, "Pkey-1", null, "RowKey-21");
            Assert.AreEqual(2, retrieved7.Count(), "#B07");

            // The next test should handle non existing table names.
            // var isSuccess = false;

            // TODO: Looks like something is not finished here
            var emptyEnumeration = this.TableStorage.Get<object>("IAmNotATable", "IaMNotAPartiTion");

            Assert.AreEqual(0, emptyEnumeration.Count(), "#B08");
        }

        /// <summary>
        /// Inserts the update and delete single thread.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [Test]
        public void InsertUpdateAndDeleteSingleThread()
        {
            const string tableName = "myTable";
            const string NewTableName = "myNewTable";

            this.TableStorage.CreateTable(tableName);

            const int PartitionCount = 10;

            var entities =
                Enumerable.Range(0, 100).Select(
                    i =>
                    new CloudEntity<object>
                        {
                            PartitionKey = string.Format("Pkey-{0}", (i % PartitionCount).ToString("0")),
                            RowKey = string.Format("RowKey-{0}", i.ToString("00")),
                            Value = new object()
                        });
            this.TableStorage.Insert(tableName, entities);

            var isSucces = false;
            try
            {
                this.TableStorage.Insert(
                    tableName, new[] { new CloudEntity<object> { PartitionKey = "Pkey-6", RowKey = "RowKey-56" } });
            }
            catch (Exception exception)
            {
                isSucces = (exception as InvalidOperationException) != null;
            }

            Assert.IsTrue(isSucces);

            this.TableStorage.CreateTable(NewTableName);
            this.TableStorage.Insert(
                NewTableName,
                new[]
                    {
                        new CloudEntity<object> { PartitionKey = "Pkey-6", RowKey = "RowKey-56", Value = new object() } 
                    });

            Assert.AreEqual(2, this.TableStorage.GetTables().Count());

            this.TableStorage.Update(
                NewTableName,
                new[] { new CloudEntity<object> { PartitionKey = "Pkey-6", RowKey = "RowKey-56", Value = 2000 } },
                true);
            Assert.AreEqual(
                2000, (int)this.TableStorage.Get<object>(NewTableName, "Pkey-6", new[] { "RowKey-56" }).First().Value);

            this.TableStorage.Delete<object>(NewTableName, "Pkey-6", new[] { "RowKey-56" });

            var retrieved = this.TableStorage.Get<object>(NewTableName);
            Assert.AreEqual(0, retrieved.Count());
        }

        /// <summary>
        /// Tears down.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TearDown]
        public void TearDown()
        {
            foreach (var tableName in this.TableStorage.GetTables().Distinct().ToList())
            {
                this.TableStorage.DeleteTable(tableName);
            }
        }

        #endregion
    }
}