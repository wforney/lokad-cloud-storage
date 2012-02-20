#region Copyright (c) Lokad 2009-2010

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class for strongly typed hierarchical references to blobs of a strongly typed content.
    /// </summary>
    /// <typeparam name="T">
    /// Type contained in the blob. 
    /// </typeparam>
    [Serializable]
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    public abstract class BlobName<T> : UntypedBlobName, IBlobLocationAndType<T>
    {
    }
}