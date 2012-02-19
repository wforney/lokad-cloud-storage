﻿#region Copyright (c) Lokad 2010-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml.Linq;

    using Lokad.Cloud.Storage.Shared.Monads;

    /// <summary>
    /// The data serializer extensions.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal static class DataSerializerExtensions
    {
        #region Public Methods and Operators

        /// <summary>
        /// Tries the deserialize.
        /// </summary>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// The result.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Result<object, Exception> TryDeserialize(
            this IDataSerializer serializer, Stream source, Type type)
        {
            var position = source.Position;
            try
            {
                var result = serializer.Deserialize(source, type);
                if (result == null)
                {
                    return Result<object, Exception>.CreateError(new SerializationException("Serializer returned null"));
                }

                var actualType = result.GetType();
                if (!type.IsAssignableFrom(actualType))
                {
                    return
                        Result<object, Exception>.CreateError(
                            new InvalidCastException(
                                string.Format(
                                    "Source was expected to be of type {0} but was of type {1}.", 
                                    type.Name, 
                                    actualType.Name)));
                }

                return Result<object, Exception>.CreateSuccess(result);
            }
            catch (Exception e)
            {
                return Result<object, Exception>.CreateError(e);
            }
            finally
            {
                source.Position = position;
            }
        }

        /// <summary>
        /// Tries to deserialize as T.
        /// </summary>
        /// <typeparam name="T">
        /// The type to try to deserialize as.
        /// </typeparam>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <returns>
        /// The result.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Result<T, Exception> TryDeserializeAs<T>(this IDataSerializer serializer, Stream source)
        {
            var position = source.Position;
            try
            {
                var result = serializer.Deserialize(source, typeof(T));
                if (result == null)
                {
                    return Result<T, Exception>.CreateError(new SerializationException("Serializer returned null"));
                }

                if (!(result is T))
                {
                    return
                        Result<T, Exception>.CreateError(
                            new InvalidCastException(
                                string.Format(
                                    "Source was expected to be of type {0} but was of type {1}.", 
                                    typeof(T).Name, 
                                    result.GetType().Name)));
                }

                return Result<T, Exception>.CreateSuccess((T)result);
            }
            catch (Exception e)
            {
                return Result<T, Exception>.CreateError(e);
            }
            finally
            {
                source.Position = position;
            }
        }

        /// <summary>
        /// Tries to deserialize as T.
        /// </summary>
        /// <typeparam name="T">
        /// The type to try to deserialize as.
        /// </typeparam>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <returns>
        /// The result.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Result<T, Exception> TryDeserializeAs<T>(this IDataSerializer serializer, byte[] source)
        {
            using (var stream = new MemoryStream(source))
            {
                return TryDeserializeAs<T>(serializer, stream);
            }
        }

        /// <summary>
        /// Tries the unpack XML.
        /// </summary>
        /// <param name="serializer">
        /// The serializer. 
        /// </param>
        /// <param name="source">
        /// The source. 
        /// </param>
        /// <returns>
        /// The result.
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static Result<XElement, Exception> TryUnpackXml(
            this IIntermediateDataSerializer serializer, Stream source)
        {
            var position = source.Position;
            try
            {
                var result = serializer.UnpackXml(source);
                if (result == null)
                {
                    return
                        Result<XElement, Exception>.CreateError(new SerializationException("Serializer returned null"));
                }

                return Result<XElement, Exception>.CreateSuccess(result);
            }
            catch (Exception e)
            {
                return Result<XElement, Exception>.CreateError(e);
            }
            finally
            {
                source.Position = position;
            }
        }

        #endregion
    }
}