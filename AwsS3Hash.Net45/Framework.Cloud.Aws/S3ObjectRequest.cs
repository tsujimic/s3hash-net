using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Framework.Cloud.Aws
{
    class S3ObjectRequest
    {
        public string BucketName
        {
            get;
            set;
        }

        public string Key
        {
            get;
            set;
        }

        public long Start
        {
            get;
            set;
        }

        public long End
        {
            get;
            set;
        }

        //public long Length
        //{
        //    get
        //    {
        //        return End - Start + 1;
        //    }
        //}
    }
}
