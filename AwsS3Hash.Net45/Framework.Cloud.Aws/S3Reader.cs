using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

using Framework;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace Framework.Cloud.Aws
{
    public class S3Reader : IReaderCloser, IDisposable
    {
        public string BucketName { get; private set; }
        public string KeyName { get; private set; }
        public long Length { get; set; }
        public int Parallels { get; private set; }
        public int Capacity { get; set; }
        public int RetryRequests { get; set; }

        private AmazonS3Client _s3Client;
        private bool _isDisposed = false;

        readonly object WAIT_FOR_COMPLETION_LOCK = new object();
        readonly object QUEUE_ACECSS_LOCK = new object();
        Queue<S3ObjectRequest> _objectRequests = new Queue<S3ObjectRequest>();
        Queue<S3ObjectRequest> _objectRequestToDownload = new Queue<S3ObjectRequest>();

        readonly object CACHE_ACECSS_LOCK = new object();
        SortedList<long, byte[]> _objectPartsCache = new SortedList<long, byte[]>();

        DownloadPartInvoker[] _downloadPartInvoker;
        Thread[] _executedThreads;
        Timer _timer;

        int _maxParts;
        long _readBytes;
        int _dueTime;
        int _origin;
        byte[] _buffer;
        bool _isOpen;

        public S3Reader()
            : this(5 * 1048576, 1)
        {
        }

        public S3Reader(int capacity, int parallels)
        {
            this.Capacity = Math.Min(Math.Max(5 * 1048576, capacity), 100 * 1048576);
            this.Parallels = Math.Min(Math.Max(parallels, 1), 16);
            this.RetryRequests = 5;

            _maxParts = this.Parallels * 3;
            _readBytes = 0;
            _dueTime = 100;
            _origin = 0;
            _buffer = null;
            _isOpen = false;
        }

        public S3Reader(string accessKey, string secretAccessKey, string path)
            : this()
        {
            Open(accessKey, secretAccessKey, path);
        }

        public S3Reader(string accessKey, string secretAccessKey, string path, int capacity, int parallels)
            : this(capacity, parallels)
        {
            Open(accessKey, secretAccessKey, path);
        }


        public void Dispose()
        {
            Dispose(true);
            //! Take yourself off the Finalization queue 
            //! to prevent finalization code for this object
            //! from executing a second time.
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            //! Check to see if Dispose has already been called.
            if (!this._isDisposed)
            {
                //! If disposing equals true, dispose all managed  and unmanaged resources.
                if (disposing)
                {
                    //! Dispose managed resources.
                    Close();
                }

                //! Release unmanaged resources.
                //! If disposing is false, only the following code is executed.

                //! Note that this is not thread safe.
                //! Another thread could start disposing the object
                //! after the managed resources are disposed,
                //! but before the disposed flag is set to true.
                //! If thread safety is necessary, it must be
                //! implemented by the client.
            }

            this._isDisposed = true;
        }

        public bool CanRead
        {
            get
            { 
                return _isOpen;
            }
        }

        public void Open(string accessKey, string secretAccessKey, string path)
        {
            string bucketName, keyName;
            if (!S3Path.TryParse(path, out bucketName, out keyName))
                throw new ArgumentException("path");
            if (string.IsNullOrEmpty(bucketName))
                throw new ArgumentException("bucketName");
            if (string.IsNullOrEmpty(keyName))
                throw new ArgumentException("keyName");

            this.BucketName = bucketName;
            this.KeyName = keyName;

            if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretAccessKey))
                _s3Client = S3Client.GetInstance(bucketName);
            else
                _s3Client = S3Client.GetInstance(accessKey, secretAccessKey, bucketName);

            GetObjectMetadataResponse getObjectMetadataResponse = getObjectMetadata(_s3Client, bucketName, keyName);
            this.Length = getObjectMetadataResponse.ContentLength;

            _objectRequests.Clear();
            _objectRequestToDownload.Clear();
            _objectPartsCache.Clear();

            S3ObjectRequest[] objectRequests = getObjectRequests(bucketName, keyName, this.Length, this.Capacity);
            foreach (S3ObjectRequest objectRequest in objectRequests)
                _objectRequests.Enqueue(objectRequest);

            _isOpen = true;
            _readBytes = 0;

            startThread();

            _timer = new Timer(timerCallBack, null, Timeout.Infinite, Timeout.Infinite);
            startTimer();
        }

        public int Read(byte[] buffer, int index, int count)
        {
            if (!this.CanRead)
                throw new ObjectDisposedException(null, "FileNotOpen");
            if (buffer == null)
                throw new ArgumentNullException("buffer");
            if (index < 0 || index > buffer.Length)
                throw new ArgumentOutOfRangeException("index");
            if (count < 0 || count > buffer.Length - index)
                throw new ArgumentOutOfRangeException("count");

            if (_buffer == null)
            {
                _origin = 0;
                _buffer = getCache();
                if (_buffer == null)
                    return 0;
            }

            int length = Math.Min(count, _buffer.Length - _origin);
            Array.Copy(_buffer, _origin, buffer, index, length);
            _origin += length;

            if (_origin >= _buffer.Length)
                _buffer = null;

            return length;
        }

        void startTimer()
        {
            if (_timer != null)
                _timer.Change(_dueTime, Timeout.Infinite);
        }

        void stopTimer()
        {
            if (_timer != null)
                _timer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        void timerCallBack(object state)
        {
            try
            {
                if (_objectPartsCache.Count < _maxParts)
                    addRequestToDownload(1);
            }
            finally
            {
                startTimer();
            }
        }

        GetObjectMetadataResponse getObjectMetadata(IAmazonS3 amazonS3, string bucketName, string keyName)
        {
            GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest()
            {
                BucketName = bucketName,
                Key = keyName
            };

            GetObjectMetadataResponse objectMetadataResponse = amazonS3.GetObjectMetadata(objectMetadataRequest);
            return objectMetadataResponse;
        }

        int addRequestToDownload(int count)
        {
            lock (QUEUE_ACECSS_LOCK)
            {
                int n = 0;
                while (n < count)
                {
                    if (_objectRequests.Count == 0)
                        break;

                    S3ObjectRequest request = _objectRequests.Dequeue();
                    _objectRequestToDownload.Enqueue(request);
                    addCache(request.Start, null);

                    n++;
                }

                return n;
            }
        }

        void finishedPartsToDownload()
        {
            lock (QUEUE_ACECSS_LOCK)
            {
                while (_objectRequests.Count > 0 || _readBytes < Length)
                {
                    Monitor.Wait(QUEUE_ACECSS_LOCK, 100);
                    checkLastException();
                }
            }
        }

        void checkLastException()
        {
            foreach (DownloadPartInvoker invoker in _downloadPartInvoker)
                if (invoker.LastException != null)
                    throw invoker.LastException;
        }

        void addCache(long key, byte[] value)
        {
            lock (CACHE_ACECSS_LOCK)
            {
                if (_objectPartsCache.ContainsKey(key))
                    _objectPartsCache[key] = value;
                else
                    _objectPartsCache.Add(key, value);
            }
        }

        byte[] getCache()
        {
            lock (CACHE_ACECSS_LOCK)
            {
                //_lock.AcquireWriterLock(System.Threading.Timeout.Infinite);
                //_lock.ReleaseWriterLock();
                // Interlocked.Read(ref _readBytes)
                // Interlocked.Add(ref _readBytes, buffer.Length);
                while (_readBytes < Length)
                {
                    if (_objectPartsCache.Count > 0)
                    {
                        KeyValuePair<long, byte[]> pair = _objectPartsCache.First();
                        if (pair.Value != null)
                        {
                            int length = pair.Value.Length;
                            byte[] buffer = new byte[length];
                            Array.Copy(pair.Value, buffer, length);
                            _readBytes += buffer.Length;
                            _objectPartsCache.Remove(pair.Key);
                            return buffer;
                        }
                    }

                    Monitor.Wait(CACHE_ACECSS_LOCK, 200);
                }

                return null;
            }
        }

        public void WaitOne()
        {
            finishedPartsToDownload();
        }

        public void Close()
        {
            stopThread();
            stopTimer();

            _s3Client.Dispose();
            _isOpen = false;
        }

        S3ObjectRequest[] getObjectRequests(string bucketName, string keyName, long contentLength, long partSize)
        {
            if (partSize <= 0)
                throw new ArgumentOutOfRangeException("partSize");

            List<S3ObjectRequest> objectRequests = new List<S3ObjectRequest>();
            long filePosition = 0;
            long fileLength;

            for (int i = 0; filePosition < contentLength; i++)
            {
                fileLength = Math.Min(partSize, (contentLength - filePosition));

                long start = filePosition;
                long end = filePosition + fileLength - 1;

                objectRequests.Add(new S3ObjectRequest()
                {
                    BucketName = bucketName,
                    Key = keyName,
                    Start = start,
                    End = end
                });

                filePosition += fileLength;
            }

            return objectRequests.ToArray();
        }

        void startThread()
        {
            int retryRequests = this.RetryRequests;
            int parallels = this.Parallels;
            _downloadPartInvoker = new DownloadPartInvoker[parallels];
            _executedThreads = new Thread[parallels];

            for (int i = 0; i < parallels; i++)
            {
                _downloadPartInvoker[i] = new DownloadPartInvoker(_s3Client, this, retryRequests);

                Thread thread = new Thread(new ThreadStart(_downloadPartInvoker[i].Execute));
                thread.Name = "downloadPartInvoker " + i;
                thread.IsBackground = true;
                _executedThreads[i] = thread;

                thread.Start();
            }
        }

        void stopThread()
        {
            if (_downloadPartInvoker != null)
                foreach (DownloadPartInvoker downloadPartInvoker in _downloadPartInvoker)
                    downloadPartInvoker.Stop();

            if  (_executedThreads != null)
                foreach (Thread thread in _executedThreads)
                    thread.Join();
        }

        void abortThread()
        {
            bool anyAlive = true;
            for (int i = 0; anyAlive && i < 5; i++)
            {
                anyAlive = false;
                foreach (Thread thread in _executedThreads)
                {
                    try
                    {
                        if (thread.IsAlive)
                        {
                            thread.Abort();
                            anyAlive = true;
                        }
                    }
                    catch { }
                }
            }
        }

        class DownloadPartInvoker
        {
            IAmazonS3 _s3Client;
            S3Reader _s3Reader;
            Exception _lastException;
            bool _waitEvent;
            int _retryRequests = 0;

            internal DownloadPartInvoker(IAmazonS3 s3Client, S3Reader s3Reader)
                : this(s3Client, s3Reader, 0)
            {
            }

            internal DownloadPartInvoker(IAmazonS3 s3Client, S3Reader s3Reader, int retryRequests)
            {
                _s3Client = s3Client;
                _s3Reader = s3Reader;
                _retryRequests = retryRequests;
                _waitEvent = true;
            }

            internal Exception LastException
            {
                get
                {
                    return _lastException;
                }
            }

            S3ObjectRequest getNextPartRequest()
            {
                lock (_s3Reader.QUEUE_ACECSS_LOCK)
                {
                    if (_s3Reader._objectRequestToDownload.Count == 0)
                    {
                        Monitor.Wait(_s3Reader.QUEUE_ACECSS_LOCK, 100);
                        return null;
                    }

                    return _s3Reader._objectRequestToDownload.Dequeue();
                }
            }

            internal void Stop()
            {
                _waitEvent = false;
            }

            internal void Execute()
            {
                while (_waitEvent)
                {
                    S3ObjectRequest request = null;
                    while ((request = getNextPartRequest()) != null)
                    {
                        downloadPart(request);

                        if (_lastException != null)
                        {
                            lock (_s3Reader.WAIT_FOR_COMPLETION_LOCK)
                            {
                                Monitor.Pulse(_s3Reader.WAIT_FOR_COMPLETION_LOCK);
                            }

                            return;
                        }
                    }
                }
            }

            internal void downloadPart(S3ObjectRequest request)
            {
                Exception exception = null;

                try
                {
                    getObject(_s3Client, request);
                    exception = null;
                    return;
                }
                catch (ThreadAbortException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    exception = ex;
                }

                if (exception != null)
                    _lastException = exception;
            }

            internal void getObject(IAmazonS3 amazonS3, S3ObjectRequest request)
            {
                Exception exception = null;
                long length = request.End - request.Start + 1;

                GetObjectRequest getObjectRequest = new GetObjectRequest()
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    ByteRange = new ByteRange(request.Start, request.End)
                };

                for (int i = 0; i <= _retryRequests; i++)
                {
                    try
                    {
                        GetObjectResponse response = amazonS3.GetObject(getObjectRequest);
                        using (BufferedStream input = new BufferedStream(response.ResponseStream))
                        {
                            byte[] buffer = new byte[length];
                            int current = 0;
                            int bytesRead = 0;
                            int count = buffer.Length;
                            while ((bytesRead = input.Read(buffer, current, count)) > 0)
                            {
                                current += bytesRead;
                                count -= bytesRead;
                            }

                            _s3Reader.addCache(request.Start, buffer);
                        }

                        response.Dispose();
                        exception = null;
                        return;
                    }
                    catch (Exception ex)
                    {
                        exception = ex;

                        //! exponential backoff
                        //! wait seconds 1, 2, 4, 8, 16, 32, 64 ....
                        int millisecondsTimeout = (2 ^ i) * 1000;
                        Thread.Sleep(millisecondsTimeout);
                    }
                }

                if (exception != null)
                    throw exception;
            }

            //internal byte[] read(Stream input, long length)
            //{
            //    byte[] buffer = new byte[length];
            //    int current = 0;
            //    int bytesRead = 0;
            //    int count = buffer.Length;
            //    while ((bytesRead = input.Read(buffer, current, count)) > 0)
            //    {
            //        current += bytesRead;
            //        count -= bytesRead;
            //    }
            //    return buffer;
            //}

        }


    }
}
