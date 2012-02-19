﻿#region Copyright (c) Lokad 2009-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System;
using NUnit.Framework;

namespace Lokad.Cloud.Storage.Test.Tables
{
    using Lokad.Cloud.Storage.Tables;

    [TestFixture]
    public class CloudTableTests
    {
        [Test]
        public void TableNameValidation()
        {
            var mockProvider = CloudStorage.ForInMemoryStorage().BuildTableStorage();

            new CloudTable<int>(mockProvider, "abc"); // name is OK

            try
            {
                new CloudTable<int>(mockProvider, "ab"); // name too short
                Assert.Fail("#A00");
            }
            catch (ArgumentException) { }

            try
            {
                new CloudTable<int>(mockProvider, "ab-sl"); // hyphen not permitted
                Assert.Fail("#A01");
            }
            catch (ArgumentException) { }
        }

    }
}
