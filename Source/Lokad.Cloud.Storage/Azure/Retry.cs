#region Copyright (c) Lokad 2009-2011

// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

namespace Lokad.Cloud.Storage.Azure
{
    using System;
    using System.Threading;

    using Microsoft.WindowsAzure.StorageClient;

    /// <summary>
    /// The retry.
    /// </summary>
    /// <remarks>
    /// </remarks>
    internal static class Retry
    {
        #region Public Methods and Operators

        /// <summary>
        /// Does the specified retry policy.
        /// </summary>
        /// <param name="retryPolicy">
        /// The retry policy. 
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void Do(this RetryPolicy retryPolicy, CancellationToken cancellationToken, Action action)
        {
            var policy = retryPolicy();
            var retryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    action();
                    return;
                }
                catch (Exception exception)
                {
                    TimeSpan delay;
                    if (policy(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Does the specified first policy.
        /// </summary>
        /// <param name="firstPolicy">
        /// The first policy. 
        /// </param>
        /// <param name="secondPolicy">
        /// The second policy. 
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <remarks>
        /// </remarks>
        public static void Do(
            this RetryPolicy firstPolicy, RetryPolicy secondPolicy, CancellationToken cancellationToken, Action action)
        {
            var first = firstPolicy();
            var second = secondPolicy();
            var retryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    action();
                    return;
                }
                catch (Exception exception)
                {
                    TimeSpan delay;
                    if (first(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    if (second(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Does the until true.
        /// </summary>
        /// <param name="retryPolicy">
        /// The retry policy. 
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <remarks>
        /// Policy must support exceptions being null.
        /// </remarks>
        public static void DoUntilTrue(
            this RetryPolicy retryPolicy, CancellationToken cancellationToken, Func<bool> action)
        {
            var policy = retryPolicy();
            var retryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    if (action())
                    {
                        return;
                    }

                    TimeSpan delay;
                    if (policy(retryCount, null, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw new TimeoutException("Failed to reach a successful result in a limited number of retrials");
                }
                catch (Exception exception)
                {
                    TimeSpan delay;
                    if (policy(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Gets the specified retry policy.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="retryPolicy">
        /// The retry policy. 
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static T Get<T>(this RetryPolicy retryPolicy, CancellationToken cancellationToken, Func<T> action)
        {
            var policy = retryPolicy();
            var retryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var result = action();
                    return result;
                }
                catch (Exception exception)
                {
                    TimeSpan delay;
                    if (policy(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Gets the specified first policy.
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="firstPolicy">
        /// The first policy. 
        /// </param>
        /// <param name="secondPolicy">
        /// The second policy. 
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token. 
        /// </param>
        /// <param name="action">
        /// The action. 
        /// </param>
        /// <returns>
        /// </returns>
        /// <remarks>
        /// </remarks>
        public static T Get<T>(
            this RetryPolicy firstPolicy, RetryPolicy secondPolicy, CancellationToken cancellationToken, Func<T> action)
        {
            var first = firstPolicy();
            var second = secondPolicy();
            var retryCount = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var result = action();
                    return result;
                }
                catch (Exception exception)
                {
                    TimeSpan delay;
                    if (first(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    if (second(retryCount, exception, out delay))
                    {
                        retryCount++;
                        if (delay > TimeSpan.Zero)
                        {
                            Thread.Sleep(delay);
                        }

                        continue;
                    }

                    throw;
                }
            }
        }

        #endregion
    }
}