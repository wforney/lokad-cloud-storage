#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Blobs
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;

    /// <summary>
    /// Base class for untyped hierarchical blob names. Implementations should not inherit <see cref="UntypedBlobName"/> c&gt; but <see cref="BlobName{T}"/> instead.
    /// </summary>
    /// <remarks>
    /// </remarks>
    [Serializable]
    [DataContract(Namespace = "http://schemas.lokad.com/lokad-cloud/storage/2.0")]
    public abstract class UntypedBlobName : IBlobLocation
    {
        #region Constants and Fields

        /// <summary>
        ///   Sortable pattern for date times.
        /// </summary>
        public const string DateFormatInBlobName = "yyyy-MM-dd-HH-mm-ss";

        /// <summary>
        ///   The parsers.
        /// </summary>
        private static readonly Dictionary<Type, Func<string, object>> Parsers =
            new Dictionary<Type, Func<string, object>>();

        /// <summary>
        ///   The printers.
        /// </summary>
        private static readonly Dictionary<Type, Func<object, string>> Printers =
            new Dictionary<Type, Func<object, string>>();

        #endregion

        #region Constructors and Destructors

        /// <summary>
        ///   Initializes static members of the <see cref="UntypedBlobName" /> class. Initializes a new instance of the <see
        ///    cref="T:System.Object" /> class.
        /// </summary>
        /// <remarks>
        /// </remarks>
        static UntypedBlobName()
        {
            // adding overrides

            // Guid: does not have default converter
            Printers.Add(typeof(Guid), o => ((Guid)o).ToString("N"));
            Parsers.Add(typeof(Guid), s => new Guid(s));

            // DateTime: sortable ascending;
            // NOTE: not time zone safe, users have to deal with that themselves
            Printers.Add(
                typeof(DateTime), o => ((DateTime)o).ToString(DateFormatInBlobName, CultureInfo.InvariantCulture));
            Parsers.Add(
                typeof(DateTime), s => DateTime.ParseExact(s, DateFormatInBlobName, CultureInfo.InvariantCulture));

            // DateTimeOffset: sortable ascending;
            // time zone safe, but always returned with UTC/zero offset (comparisons can deal with that)
            Printers.Add(
                typeof(DateTimeOffset), 
                o => ((DateTimeOffset)o).UtcDateTime.ToString(DateFormatInBlobName, CultureInfo.InvariantCulture));
            Parsers.Add(
                typeof(DateTimeOffset), 
                s =>
                new DateTimeOffset(
                    DateTime.SpecifyKind(
                        DateTime.ParseExact(s, DateFormatInBlobName, CultureInfo.InvariantCulture), DateTimeKind.Utc)));
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets the name of the container where the blob is located.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public abstract string ContainerName { get; }

        /// <summary>
        ///   Gets the location of the blob inside of the container.
        /// </summary>
        /// <remarks>
        /// </remarks>
        public virtual string Path
        {
            get
            {
                return this.ToString();
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Parse a hierarchical blob name.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="value">
        /// The value. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static T Parse<T>(string value) where T : UntypedBlobName
        {
            return ConverterTypeCache<T>.Parse(value);
        }

        /// <summary>
        /// Do not use directly, call <see cref="ToString"/> instead.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="instance">
        /// The instance. 
        /// </param>
        /// <returns>
        /// The print. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static string Print<T>(T instance) where T : UntypedBlobName
        {
            return ConverterTypeCache<T>.Print(instance);
        }

        /// <summary>
        /// Syntactic equivalent to Print{T} with T being the current base type.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override string ToString()
        {
            // Invoke a Static Generic Method using Reflection
            // because type is programmatically defined
            var method = typeof(UntypedBlobName).GetMethod("Print", BindingFlags.Static | BindingFlags.Public);

            // Binding the method info to generic arguments
            method = method.MakeGenericMethod(new[] { this.GetType() });

            // Invoking the method and passing parameters
            // The null parameter is the object to call the method from. Since the method is static, pass null.
            return (string)method.Invoke(null, new object[] { this });
        }

        #endregion

        #region Methods

        /// <summary>
        /// Returns <paramref name="defaultValue"/> if the given <paramref name="key"/> is not present within the dictionary.
        /// </summary>
        /// <typeparam name="TKey">
        /// The type of the key. 
        /// </typeparam>
        /// <typeparam name="TValue">
        /// The type of the value. 
        /// </typeparam>
        /// <param name="self">
        /// The dictionary. 
        /// </param>
        /// <param name="key">
        /// The key to look for. 
        /// </param>
        /// <param name="defaultValue">
        /// The default value. 
        /// </param>
        /// <returns>
        /// value matching <paramref name="key"/> or <paramref name="defaultValue"/> if none is found 
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static TValue GetValue<TKey, TValue>(IDictionary<TKey, TValue> self, TKey key, TValue defaultValue)
        {
            TValue value;
            if (self.TryGetValue(key, out value))
            {
                return value;
            }

            return defaultValue;
        }

        /// <summary>
        /// Internals the parse.
        /// </summary>
        /// <param name="value">
        /// The value. 
        /// </param>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// The internal parse. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static object InternalParse(string value, Type type)
        {
            var func = GetValue(Parsers, type, s => Convert.ChangeType(s, Nullable.GetUnderlyingType(type) ?? type));
            return func(value);
        }

        /// <summary>
        /// Internals the print.
        /// </summary>
        /// <param name="value">
        /// The value. 
        /// </param>
        /// <param name="type">
        /// The type. 
        /// </param>
        /// <returns>
        /// The internal print. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        private static string InternalPrint(object value, Type type)
        {
            var func = GetValue(Printers, type, o => o.ToString());
            return func(value);
        }

        #endregion

        /// <summary>
        /// The converter type cache.
        /// </summary>
        /// <typeparam name="T">
        /// The type.
        /// </typeparam>
        /// <remarks>
        /// </remarks>
        private class ConverterTypeCache<T>
        {
            #region Constants and Fields

            /// <summary>
            ///   The delimeter.
            /// </summary>
            private const string Delimeter = "/";

            /// <summary>
            ///   The members.
            /// </summary>
            private static readonly MemberInfo[] Members; // either 'FieldInfo' or 'PropertyInfo'

            /// <summary>
            ///   The treat default as null.
            /// </summary>
            private static readonly bool[] TreatDefaultAsNull;

            #endregion

            #region Constructors and Destructors

            /// <summary>
            ///   Initializes static members of the <see cref="ConverterTypeCache" /> class.
            /// </summary>
            /// <remarks>
            /// </remarks>
            static ConverterTypeCache()
            {
                // HACK: optimize this to IL code, if needed
                // NB: this approach could be used to generate F# style objects!
                Members =
                    typeof(T).GetFields().Select(f => (MemberInfo)f).Union(typeof(T).GetProperties()).Where(
                        f => f.GetCustomAttributes(typeof(RankAttribute), true).Any())

                        // ordering always respect inheritance
                        .GroupBy(f => f.DeclaringType).OrderBy(g => g.Key, new InheritanceComparer()).Select(
                            g =>
                            g.OrderBy(
                                f => ((RankAttribute)f.GetCustomAttributes(typeof(RankAttribute), true).First()).Index)).SelectMany(f => f).ToArray();

                TreatDefaultAsNull =
                    Members.Select(
                        m =>
                        ((RankAttribute)m.GetCustomAttributes(typeof(RankAttribute), true).First()).TreatDefaultAsNull).ToArray();
            }

            #endregion

            #region Public Methods and Operators

            /// <summary>
            /// Parses the specified value.
            /// </summary>
            /// <param name="value">
            /// The value. 
            /// </param>
            /// <returns>
            /// </returns>
            /// <remarks>
            /// </remarks>
            public static T Parse(string value)
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException("value");
                }

                var split = value.Split(new[] { Delimeter }, StringSplitOptions.RemoveEmptyEntries);

                // In order to support parsing blob names also to blob name supper classes
                // in case of inheritance, we simply ignore supplementary items in the name
                if (split.Length < Members.Length)
                {
                    throw new ArgumentException(
                        "Number of items in the string is invalid. Are you missing something?", "value");
                }

                var parameters = new object[Members.Length];

                for (var i = 0; i < parameters.Length; i++)
                {
                    var memberType = Members[i] is FieldInfo
                                         ? ((FieldInfo)Members[i]).FieldType
                                         : ((PropertyInfo)Members[i]).PropertyType;

                    parameters[i] = InternalParse(split[i], memberType);
                }

                // Initialization through reflection (no assumption on constructors)
                var name = (T)FormatterServices.GetUninitializedObject(typeof(T));
                for (var i = 0; i < Members.Length; i++)
                {
                    if (Members[i] is FieldInfo)
                    {
                        ((FieldInfo)Members[i]).SetValue(name, parameters[i]);
                    }
                    else
                    {
                        ((PropertyInfo)Members[i]).SetValue(name, parameters[i], new object[0]);
                    }
                }

                return name;
            }

            /// <summary>
            /// Prints the specified instance.
            /// </summary>
            /// <param name="instance">
            /// The instance. 
            /// </param>
            /// <returns>
            /// The print. 
            /// </returns>
            /// <remarks>
            /// </remarks>
            public static string Print(T instance)
            {
                var sb = new StringBuilder();
                for (var i = 0; i < Members.Length; i++)
                {
                    var info = Members[i];
                    var fieldInfo = info as FieldInfo;
                    var propInfo = info as PropertyInfo;

                    var memberType = (null != fieldInfo)
                                         ? fieldInfo.FieldType
                                         : propInfo != null ? propInfo.PropertyType : null;
                    var value = (null != fieldInfo)
                                    ? fieldInfo.GetValue(instance)
                                    : propInfo != null ? propInfo.GetValue(instance, new object[0]) : null;

                    if (null == value || (TreatDefaultAsNull[i] && IsDefaultValue(value, memberType)))
                    {
                        // Delimiter has to be appended here to avoid enumerating
                        // too many blog (names being prefix of each other).
                        // For example, without delimiter, the prefix 'foo/123' whould enumerate both
                        // foo/123/bar
                        // foo/1234/bar
                        // Then, we should not append a delimiter if prefix is entirely empty
                        // because it would not properly enumerate all blobs (semantic associated with
                        // empty prefix).
                        if (i > 0)
                        {
                            sb.Append(Delimeter);
                        }

                        break;
                    }

                    var s = InternalPrint(value, memberType);
                    if (i > 0)
                    {
                        sb.Append(Delimeter);
                    }

                    sb.Append(s);
                }

                return sb.ToString();
            }

            #endregion

            #region Methods

            /// <summary>
            /// Determines whether [is default value] [the specified value].
            /// </summary>
            /// <param name="value">
            /// The value. 
            /// </param>
            /// <param name="type">
            /// The type. 
            /// </param>
            /// <returns>
            /// <c>true</c> if [is default value] [the specified value]; otherwise, <c>false</c> . 
            /// </returns>
            /// <remarks>
            /// </remarks>
            private static bool IsDefaultValue(object value, Type type)
            {
                if (type == typeof(string))
                {
                    return string.IsNullOrEmpty((string)value);
                }

                if (type.IsValueType)
                {
                    return Activator.CreateInstance(type).Equals(value);
                }

                return value == null;
            }

            #endregion
        }

        /// <summary>
        /// The inheritance comparer.
        /// </summary>
        /// <remarks>
        /// </remarks>
        private class InheritanceComparer : IComparer<Type>
        {
            #region Public Methods and Operators

            /// <summary>
            /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
            /// </summary>
            /// <param name="x">
            /// The first object to compare. 
            /// </param>
            /// <param name="y">
            /// The second object to compare. 
            /// </param>
            /// <returns>
            /// Value Condition Less than zerox is less than y.Zerox equals y.Greater than zerox is greater than y. 
            /// </returns>
            /// <remarks>
            /// </remarks>
            public int Compare(Type x, Type y)
            {
                if (x == y)
                {
                    return 0;
                }

                return x.IsSubclassOf(y) ? 1 : -1;
            }

            #endregion
        }
    }
}