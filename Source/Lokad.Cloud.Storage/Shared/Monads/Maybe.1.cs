// Imported from Lokad.Shared, 2011-02-08

namespace Lokad.Cloud.Storage.Shared.Monads
{
    using System;

    /// <summary>
    /// Helper class that indicates nullable value in a good-citizenship code
    /// </summary>
    /// <typeparam name="T">
    /// underlying type 
    /// </typeparam>
    /// <remarks>
    /// </remarks>
    [Serializable]
    public class Maybe<T> : IEquatable<Maybe<T>>
    {
        #region Constants and Fields

        /// <summary>
        ///   Default empty instance.
        /// </summary>
        public static readonly Maybe<T> Empty = new Maybe<T>(default(T), false);

        /// <summary>
        /// The has value.
        /// </summary>
        private readonly bool hasValue;

        /// <summary>
        /// The value.
        /// </summary>
        private readonly T value;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Maybe{T}"/> class. 
        /// Initializes a new instance of the <see cref="Maybe&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="value">
        /// The value. 
        /// </param>
        /// <remarks>
        /// </remarks>
        internal Maybe(T value)
            : this(value, true)
        {
            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            // ReSharper restore CompareNonConstrainedGenericWithNull
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Maybe{T}"/> class. 
        /// Prevents a default instance of the <see cref="Maybe&lt;T&gt;"/> class from being created.
        /// </summary>
        /// <param name="item">
        /// The item. 
        /// </param>
        /// <param name="hasValue">
        /// if set to <c>true</c> [has value]. 
        /// </param>
        /// <remarks>
        /// </remarks>
        private Maybe(T item, bool hasValue)
        {
            this.value = item;
            this.hasValue = hasValue;
        }

        #endregion

        #region Public Properties

        /// <summary>
        ///   Gets a value indicating whether this instance has value.
        /// </summary>
        /// <value> <c>true</c> if this instance has value; otherwise, <c>false</c> . </value>
        /// <remarks>
        /// </remarks>
        public bool HasValue
        {
            get
            {
                return this.hasValue;
            }
        }

        /// <summary>
        ///   Gets the underlying value.
        /// </summary>
        /// <value> The value. </value>
        /// <remarks>
        /// </remarks>
        public T Value
        {
            get
            {
                if (!this.hasValue)
                {
                    throw new InvalidOperationException("Dont access value when Maybe is empty.");
                }

                return this.value;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        ///   Implements the operator ==.
        /// </summary>
        /// <param name="left"> The left. </param>
        /// <param name="right"> The right. </param>
        /// <returns> The result of the operator. </returns>
        /// <remarks>
        /// </remarks>
        public static bool operator ==(Maybe<T> left, Maybe<T> right)
        {
            return Equals(left, right);
        }

        /// <summary>
        ///   Performs an explicit conversion from <see cref="Maybe{T}" /> to <typeparamref name="T" /> .
        /// </summary>
        /// <param name="item"> The item. </param>
        /// <returns> The result of the conversion. </returns>
        /// <remarks>
        /// </remarks>
        public static explicit operator T(Maybe<T> item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            if (!item.HasValue)
            {
                throw new ArgumentException("May be must have value");
            }

            return item.Value;
        }

        /// <summary>
        ///   Performs an implicit conversion from <typeparamref name="T" /> to <see cref="Maybe{T}" /> .
        /// </summary>
        /// <param name="item"> The item. </param>
        /// <returns> The result of the conversion. </returns>
        /// <remarks>
        /// </remarks>
        public static implicit operator Maybe<T>(T item)
        {
            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            // ReSharper restore CompareNonConstrainedGenericWithNull
            return new Maybe<T>(item);
        }

        /// <summary>
        ///   Implements the operator !=.
        /// </summary>
        /// <param name="left"> The left. </param>
        /// <param name="right"> The right. </param>
        /// <returns> The result of the operator. </returns>
        /// <remarks>
        /// </remarks>
        public static bool operator !=(Maybe<T> left, Maybe<T> right)
        {
            return !Equals(left, right);
        }

        /// <summary>
        /// Applies the specified action to the value, if it is present.
        /// </summary>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <returns>
        /// same instance for inlining 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> Apply(Action<T> action)
        {
            if (this.hasValue)
            {
                action(this.value);
            }

            return this;
        }

        /// <summary>
        /// Converts this instance to <see cref="Maybe{T}"/> , while applying <paramref name="converter"/> if there is a value.
        /// </summary>
        /// <typeparam name="TTarget">
        /// The type of the target. 
        /// </typeparam>
        /// <param name="converter">
        /// The converter. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<TTarget> Convert<TTarget>(Func<T, TTarget> converter)
        {
            return this.hasValue ? converter(this.value) : Maybe<TTarget>.Empty;
        }

        /// <summary>
        /// Retrieves converted value, using a <paramref name="defaultValue"/> if it is absent.
        /// </summary>
        /// <typeparam name="TTarget">
        /// type of the conversion target 
        /// </typeparam>
        /// <param name="converter">
        /// The converter. 
        /// </param>
        /// <param name="defaultValue">
        /// The default value. 
        /// </param>
        /// <returns>
        /// value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TTarget Convert<TTarget>(Func<T, TTarget> converter, Func<TTarget> defaultValue)
        {
            return this.hasValue ? converter(this.value) : defaultValue();
        }

        /// <summary>
        /// Retrieves converted value, using a <paramref name="defaultValue"/> if it is absent.
        /// </summary>
        /// <typeparam name="TTarget">
        /// type of the conversion target 
        /// </typeparam>
        /// <param name="converter">
        /// The converter. 
        /// </param>
        /// <param name="defaultValue">
        /// The default value. 
        /// </param>
        /// <returns>
        /// value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public TTarget Convert<TTarget>(Func<T, TTarget> converter, TTarget defaultValue)
        {
            return this.hasValue ? converter(this.value) : defaultValue;
        }

        /// <summary>
        /// Determines whether the specified <see cref="Maybe{T}"/> is equal to the current <see cref="Maybe{T}"/> .
        /// </summary>
        /// <param name="maybe">
        /// The <see cref="Maybe{T}"/> to compare with. 
        /// </param>
        /// <returns>
        /// true if the objects are equal 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public bool Equals(Maybe<T> maybe)
        {
            if (ReferenceEquals(null, maybe))
            {
                return false;
            }

            if (ReferenceEquals(this, maybe))
            {
                return true;
            }

            if (this.hasValue != maybe.hasValue)
            {
                return false;
            }

            if (!this.hasValue)
            {
                return true;
            }

            return this.value.Equals(maybe.value);
        }

        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/> .
        /// </summary>
        /// <param name="obj">
        /// The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/> . 
        /// </param>
        /// <returns>
        /// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/> ; otherwise, false. 
        /// </returns>
        /// <exception cref="T:System.NullReferenceException">
        /// The
        ///   <paramref name="obj"/>
        ///   parameter is null.
        /// </exception>
        /// <remarks>
        /// </remarks>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            var maybe = obj as Maybe<T>;
            if (maybe == null)
            {
                return false;
            }

            return this.Equals(maybe);
        }

        /// <summary>
        /// Serves as a hash function for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="Maybe{T}"/> . 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable CompareNonConstrainedGenericWithNull
                return ((this.value != null ? this.value.GetHashCode() : 0) * 397) ^ this.hasValue.GetHashCode();

                // ReSharper restore CompareNonConstrainedGenericWithNull
            }
        }

        /// <summary>
        /// Retrieves value from this instance, using a <paramref name="defaultValue"/> if it is absent.
        /// </summary>
        /// <param name="defaultValue">
        /// The default value. 
        /// </param>
        /// <returns>
        /// value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public T GetValue(Func<T> defaultValue)
        {
            return this.hasValue ? this.value : defaultValue();
        }

        /// <summary>
        /// Retrieves value from this instance, using a <paramref name="defaultValue"/> if it is absent.
        /// </summary>
        /// <param name="defaultValue">
        /// The default value. 
        /// </param>
        /// <returns>
        /// value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public T GetValue(T defaultValue)
        {
            return this.hasValue ? this.value : defaultValue;
        }

        /// <summary>
        /// Retrieves value from this instance, using a <paramref name="defaultValue"/> factory, if it is absent
        /// </summary>
        /// <param name="defaultValue">
        /// The default value to provide. 
        /// </param>
        /// <returns>
        /// maybe value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> GetValue(Func<Maybe<T>> defaultValue)
        {
            return this.hasValue ? this : defaultValue();
        }

        /// <summary>
        /// Retrieves value from this instance, using a <paramref name="defaultValue"/> if it is absent
        /// </summary>
        /// <param name="defaultValue">
        /// The default value to provide. 
        /// </param>
        /// <returns>
        /// maybe value 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> GetValue(Maybe<T> defaultValue)
        {
            return this.hasValue ? this : defaultValue;
        }

        /// <summary>
        /// Executes the specified action, if the value is absent
        /// </summary>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <returns>
        /// same instance for inlining 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public Maybe<T> Handle(Action action)
        {
            if (!this.hasValue)
            {
                action();
            }

            return this;
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance. 
        /// </returns>
        /// <remarks>
        /// </remarks>
        public override string ToString()
        {
            if (this.hasValue)
            {
                return "<" + this.value + ">";
            }

            return "<Empty>";
        }

        #endregion
    }
}