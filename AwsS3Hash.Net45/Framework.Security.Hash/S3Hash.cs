using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

using Framework.Cloud.Aws;

namespace Framework.Security.Hash
{
    public class S3Hash
    {
        const int BLOCK_SIZE = 4096;

        public static byte[] ComputeMD5(S3Reader s3)
        {
            //! MD-5
            using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider())
            {
                return Compute(s3, md5);
            }
        }

        public static byte[] ComputeSHA1(S3Reader s3)
        {
            //! SHA-1
            using (SHA1Managed sha1 = new SHA1Managed())
            {
                return Compute(s3, sha1);
            }
        }

        public static byte[] ComputeSHA256(S3Reader s3)
        {
            //! SHA-2
            using (SHA256Managed sha256 = new SHA256Managed())
            {
                return Compute(s3, sha256);
            }
        }

        public static byte[] ComputeSHA384(S3Reader s3)
        {
            //! SHA-2
            using (SHA384Managed sha384 = new SHA384Managed())
            {
                return Compute(s3, sha384);
            }
        }

        public static byte[] ComputeSHA512(S3Reader s3)
        {
            //! SHA-2
            using (SHA512Managed sha512 = new SHA512Managed())
            {
                return Compute(s3, sha512);
            }
        }

        static byte[] Compute(S3Reader s3, HashAlgorithm hashAlgorithm)
        {
            byte[] buffer = new byte[BLOCK_SIZE];
            int read = 0;
            while ((read = s3.Read(buffer, 0, BLOCK_SIZE)) > 0)
            {
                hashAlgorithm.TransformBlock(buffer, 0, read, null, 0);
            }

            hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
            return hashAlgorithm.Hash;
        }

        public delegate void ProgressDelegate(long BytesRead, long Length, ref bool Cancel);
        public event ProgressDelegate OnProgress;

        public byte[] ComputeHashMD5(S3Reader s3)
        {
            //! MD-5
            using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider())
            {
                return ComputeHash(s3, md5);
            }
        }

        public byte[] ComputeHashSHA1(S3Reader s3)
        {
            //! SHA-1
            using (SHA1Managed sha1 = new SHA1Managed())
            {
                return ComputeHash(s3, sha1);
            }
        }

        public byte[] ComputeHashSHA256(S3Reader s3)
        {
            //! SHA-2
            using (SHA256Managed sha256 = new SHA256Managed())
            {
                return ComputeHash(s3, sha256);
            }
        }

        public byte[] ComputeHashSHA384(S3Reader s3)
        {
            //! SHA-2
            using (SHA384Managed sha384 = new SHA384Managed())
            {
                return ComputeHash(s3, sha384);
            }

        }

        public byte[] ComputeHashSHA512(S3Reader s3)
        {
            //! SHA-2
            using (SHA512Managed sha512 = new SHA512Managed())
            {
                return ComputeHash(s3, sha512);
            }
        }


        public byte[] ComputeHash(S3Reader s3, HashAlgorithm hashAlgorithm)
        {
            bool cancelFlag = false;
            int bytesRead = 0, read = 0;
            long length = s3.Length;

            byte[] buffer = new byte[BLOCK_SIZE];
            while ((read = s3.Read(buffer, 0, BLOCK_SIZE)) > 0)
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

        public async Task<byte[]> ComputeHashAsync(S3Reader s3, HashAlgorithm hashAlgorithm, IProgress<int> progress)
        {
            using (_cancellationTokenSource = new CancellationTokenSource())
            {
                try
                {
                    var ret = await Task<byte[]>.Run(() =>
                    {
                        byte[] buffer = new byte[BLOCK_SIZE];

                        int bytesRead = 0, read = 0;
                        float length = s3.Length;

                        while ((read = s3.Read(buffer, 0, BLOCK_SIZE)) > 0)
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
