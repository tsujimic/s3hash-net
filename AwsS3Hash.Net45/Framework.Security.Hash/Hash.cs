using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

using Framework;

namespace Framework.Security.Hash
{
    public class Hash
    {
        const int BLOCK_SIZE = 4096;

        public static byte[] ComputeMD5(IReaderCloser r)
        {
            //! MD-5
            using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider())
            {
                return Compute(r, md5);
            }
        }

        public static byte[] ComputeSHA1(IReaderCloser r)
        {
            //! SHA-1
            using (SHA1Managed sha1 = new SHA1Managed())
            {
                return Compute(r, sha1);
            }
        }

        public static byte[] ComputeSHA256(IReaderCloser r)
        {
            //! SHA-2
            using (SHA256Managed sha256 = new SHA256Managed())
            {
                return Compute(r, sha256);
            }
        }

        public static byte[] ComputeSHA384(IReaderCloser r)
        {
            //! SHA-2
            using (SHA384Managed sha384 = new SHA384Managed())
            {
                return Compute(r, sha384);
            }
        }

        public static byte[] ComputeSHA512(IReaderCloser r)
        {
            //! SHA-2
            using (SHA512Managed sha512 = new SHA512Managed())
            {
                return Compute(r, sha512);
            }
        }

        static byte[] Compute(IReaderCloser r, HashAlgorithm hashAlgorithm)
        {
            byte[] buffer = new byte[BLOCK_SIZE];
            int read = 0;
            while ((read = r.Read(buffer, 0, BLOCK_SIZE)) > 0)
            {
                hashAlgorithm.TransformBlock(buffer, 0, read, null, 0);
            }

            hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
            return hashAlgorithm.Hash;
        }

        public delegate void ProgressDelegate(long BytesRead, long Length, ref bool Cancel);
        public event ProgressDelegate OnProgress;

        public byte[] ComputeHashMD5(IReaderCloser r)
        {
            //! MD-5
            using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider())
            {
                return ComputeHash(r, md5);
            }
        }

        public byte[] ComputeHashSHA1(IReaderCloser r)
        {
            //! SHA-1
            using (SHA1Managed sha1 = new SHA1Managed())
            {
                return ComputeHash(r, sha1);
            }
        }

        public byte[] ComputeHashSHA256(IReaderCloser r)
        {
            //! SHA-2
            using (SHA256Managed sha256 = new SHA256Managed())
            {
                return ComputeHash(r, sha256);
            }
        }

        public byte[] ComputeHashSHA384(IReaderCloser r)
        {
            //! SHA-2
            using (SHA384Managed sha384 = new SHA384Managed())
            {
                return ComputeHash(r, sha384);
            }

        }

        public byte[] ComputeHashSHA512(IReaderCloser r)
        {
            //! SHA-2
            using (SHA512Managed sha512 = new SHA512Managed())
            {
                return ComputeHash(r, sha512);
            }
        }


        public byte[] ComputeHash(IReaderCloser r, HashAlgorithm hashAlgorithm)
        {
            bool cancelFlag = false;
            int bytesRead = 0, read = 0;
            long length = r.Length;

            byte[] buffer = new byte[BLOCK_SIZE];
            while ((read = r.Read(buffer, 0, BLOCK_SIZE)) > 0)
            {
                hashAlgorithm.TransformBlock(buffer, 0, read, null, 0);
                bytesRead += read;

                cancelFlag = false;
                if (OnProgress != null)
                {
                    OnProgress(bytesRead, length, ref cancelFlag);
                    if (cancelFlag == true)
                    {
                        hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
                        return null;
                    }
                }
            }

            hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
            return hashAlgorithm.Hash;
        }


        private CancellationTokenSource _cancellationTokenSource;

        public async Task<byte[]> ComputeHashAsync(IReaderCloser r, HashAlgorithm hashAlgorithm, IProgress<int> progress)
        {
            using (_cancellationTokenSource = new CancellationTokenSource())
            {
                try
                {
                    var ret = await Task<byte[]>.Run(() =>
                    {
                        byte[] buffer = new byte[BLOCK_SIZE];

                        int bytesRead = 0, read = 0;
                        float length = r.Length;

                        while ((read = r.Read(buffer, 0, BLOCK_SIZE)) > 0)
                        {
                            _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                            hashAlgorithm.TransformBlock(buffer, 0, read, null, 0);
                            bytesRead += read;

                            int value = (int)((bytesRead / length) * 100);
                            progress.Report(value);
                        }

                        hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
                        return hashAlgorithm.Hash;

                    }, _cancellationTokenSource.Token);

                    return ret;
                }
                catch (OperationCanceledException)
                {
                    progress.Report(-1);
                    return null;
                }
            }
        }

        public void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }

    }
}
