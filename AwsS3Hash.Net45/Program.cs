using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Mono.Options;
using Framework.Cloud.Aws;
using Framework.Log;
using Framework.Security.Hash;


namespace AwsS3Hash.Net45
{
    class Program
    {
        static int Main(string[] args)
        {
            //! systemName
            //! us-east-1 The US East (Virginia) endpoint.
            //! us-west-1 The US West (N. California) endpoint.
            //! us-west-2 The US West (Oregon) endpoint.
            //! eu-west-1 The EU West (Ireland) endpoint.
            //! ap-northeast-1 The Asia Pacific (Tokyo) endpoint.
            //! ap-southeast-1 The Asia Pacific (Singapore) endpoint.
            //! ap-southeast-2 The Asia Pacific (Sydney) endpoint.
            //! sa-east-1 The South America (Sao Paulo) endpoint.
            //! us-gov-west-1 The US GovCloud West (Oregon) endpoint.
            //! cn-north-1 The China (Beijing) endpoint.

            string awsAccessKeyId = null;
            string awsSecretAccessKey = null;
            string systemName = "ap-northeast-1";
            string filePath = null;
            string hashType = "SHA1";
            int part = 5;
            int parallel = 5;
            int timeout = 300;
            string logPath = null;
            bool showHelp = false;

            OptionSet optionSet = new OptionSet()
            {
                {"a|accesskey=", "AWS access key.", v => awsAccessKeyId = v},
                {"s|secretkey=", "AWS secret access key.", v => awsSecretAccessKey = v},
                {"r|region=", "AWS region. default ap-northeast-1", v => systemName = v},
                {"p|path=", "s3 file path. ex s3://bucket/key ", v => filePath = v},
                {"type=", "compute hash type [MD5|SHA1|SHA256|SHA384|SHA512]. default SHA1", v => hashType = v},
                {"part=", "part size(MB) 5 to 100 (maximum 100MB). default 5", (int v) => part = v},
                {"parallel=", "parallel download count 1 to 64. default 5", (int v) => parallel = v},
                {"t|timeout=", "timeout(sec). default 300", (int v) => timeout = v},
                {"l|log=", "log file.", v => logPath = v},
                {"?|help", "show help.", v => showHelp = v != null}
            };

            try
            {
                List<string> extra = optionSet.Parse(args);
                //extra.ForEach(t => Console.WriteLine(t));
            }
            catch (OptionException ex)
            {
                ShowExceptionMessage(ex);
                return 1;
            }

            if (showHelp || filePath == null)
            {
                ShowUsage(optionSet);
                return 1;
            }


            //! part size : 5MB - 100MB
            int partSize = part;
            partSize = Math.Min(Math.Max(partSize, 5), 100);
            long downloadPartSize = partSize * 1024L * 1024L;

            //! parallel count : 1 - 64
            int parallelCount = Math.Min(Math.Max(parallel, 1), 64);

            try
            {
                Logger.AddConsoleTraceListener();
                Logger.AddListener(logPath);

                Logger.WriteLine("--------------------------------------------------");
                //Logger.WriteLine("region : {0}", systemName);
                Logger.WriteLine("part size (byte) : {0}", downloadPartSize);
                Logger.WriteLine("part size (MB) : {0}", partSize);
                Logger.WriteLine("parallel count : {0}", parallelCount);
                Logger.WriteLine("timeout (sec) : {0}", timeout);
                Logger.WriteLine("file path : {0}", filePath);

                DateTime startDateTime = DateTime.Now;
                Logger.WriteLine("start datetime : {0}", startDateTime.ToString("yyyy-MM-dd HH:mm:ss:fff"));

                Stopwatch sw = new Stopwatch();
                sw.Start();

                S3Client.DefaultRegion = systemName;
                S3Client.Timeout = TimeSpan.FromSeconds(timeout);

                byte[] result;
                using (S3Reader s3 = new S3Reader(awsAccessKeyId, awsSecretAccessKey, filePath, partSize, parallelCount))
                {
                    Logger.WriteLine("input : {0} length : {1} byte", filePath, s3.Length.ToString("#,0"));

                    if (string.Compare(hashType, "SHA1", true) == 0)
                    {
                        Logger.WriteLine("hash : SHA1");
                        result = Hash.ComputeSHA1(s3);
                    }
                    else if (string.Compare(hashType, "SHA256", true) == 0)
                    {
                        Logger.WriteLine("hash : SHA256");
                        result = Hash.ComputeSHA256(s3);
                    }
                    else if (string.Compare(hashType, "SHA384", true) == 0)
                    {
                        Logger.WriteLine("hash : SHA384");
                        result = Hash.ComputeSHA384(s3);
                    }
                    else if (string.Compare(hashType, "SHA512", true) == 0)
                    {
                        Logger.WriteLine("hash : SHA512");
                        result = Hash.ComputeSHA512(s3);
                    }
                    else
                    {
                        Logger.WriteLine("hash : MD5");
                        result = Hash.ComputeMD5(s3);
                    }
                }

                sw.Stop();
                Logger.WriteLine("Stopwatch : {0}({1} msec)", sw.Elapsed, sw.Elapsed.TotalMilliseconds);

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < result.Length; i++)
                    sb.Append(result[i].ToString("x2"));

                Logger.WriteLine("compute (binary) : {0}", sb.ToString());
                Logger.WriteLine("compute (base64) : {0}", Convert.ToBase64String(result));
                return 0;
            }
            catch (Exception ex)
            {
                Logger.WriteLine(ex.Message);
                return 1;
            }
            finally
            {
                Logger.ClearListener();
            }
        }


        static void ShowExceptionMessage(OptionException optionException)
        {
            Console.Error.Write("{0}: ", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
            Console.Error.WriteLine(optionException.Message);
            Console.Error.WriteLine("Try `{0} --help' for more information.", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
        }

        static void ShowUsage(OptionSet optionSet)
        {
            Console.Error.WriteLine("Usage: {0} [options]", System.Reflection.Assembly.GetEntryAssembly().GetName().Name);
            Console.Error.WriteLine();
            Console.Error.WriteLine("Options:");
            optionSet.WriteOptionDescriptions(Console.Error);
        }

    }
}
