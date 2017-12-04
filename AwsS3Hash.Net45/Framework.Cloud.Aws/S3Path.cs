using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Framework.Cloud.Aws
{
    class S3Path
    {
        //public static readonly char PathSeparator = '/';
        //private static readonly string RegexPattern = @"(?<prefix>^s3\://)?(?<bucket>[\-_.a-z0-9]+)[\\/](?<key>.+)";
        private static readonly string RegexPattern = @"(?<prefix>^s3\://)?(?<bucket>[\-_.a-z0-9]+)[\\/]*(?<key>.*)";

        public static bool IsValid(string input)
        {
            return Regex.Match(input, RegexPattern, RegexOptions.IgnoreCase).Success;
            //Regex regex = new Regex(RegexPattern, RegexOptions.IgnoreCase);
            //Match match = regex.Match(input);
            //return match.Success;
        }

        public static bool TryParse(string input, out string bucketName, out string keyName)
        {
            if (string.IsNullOrEmpty(input))
                throw new ArgumentException("input");

            Match match = Regex.Match(input, RegexPattern, RegexOptions.IgnoreCase);
            if (match.Success)
            {
                //string prefixName = match.Groups["prefix"].Value;
                bucketName = match.Groups["bucket"].Value;
                keyName = match.Groups["key"].Value;
                //string fileName = Path.GetFileName(objectName);
                //string dirName = Path.GetDirectoryName(objectName);
            }
            else
            {
                bucketName = string.Empty;
                keyName = string.Empty;
            }

            return match.Success;
        }

        public static string GetBucketName(string input)
        {
            if (string.IsNullOrEmpty(input))
                throw new ArgumentException("input");

            Match match = Regex.Match(input, RegexPattern, RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new ArgumentException("input");

            return match.Groups["bucket"].Value;
        }

        public static string GetKeyName(string input)
        {
            if (string.IsNullOrEmpty(input))
                throw new ArgumentException("input");

            Match match = Regex.Match(input, RegexPattern, RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new ArgumentException("input");

            return match.Groups["key"].Value;
        }

        public static string GetFileName(string input)
        {
            if (string.IsNullOrEmpty(input))
                throw new ArgumentException("input");

            Match match = Regex.Match(input, RegexPattern, RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new ArgumentException("input");

            string objectName = match.Groups["key"].Value;
            return Path.GetFileName(objectName);
        }

    }

}
