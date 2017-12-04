using System;
using System.Collections.Generic;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

using Framework.Log;

namespace Framework.Cloud.Aws
{
    class S3Client
    {
        //public static string ServiceUrl = "https://s3-us-west-2.amazonaws.com/";
        //public static string ServiceUrl = "https://s3.amazonaws.com/";
        //public static string ServiceUrl = "https://s3-ap-northeast-1.amazonaws.com/";
        public static TimeSpan Timeout = TimeSpan.FromMinutes(5);
        public static string ProfileName = "default";
        public static string DefaultRegion = "ap-northeast-1";
        //public static RegionEndpoint DefaultRegion = RegionEndpoint.APNortheast1;

        public static AmazonS3Client GetInstance()
        {
            CredentialProfile basicProfile;
            SharedCredentialsFile sharedFile = new SharedCredentialsFile();
            if (sharedFile.TryGetProfile(ProfileName, out basicProfile))
            {
                AWSCredentials awsCredentials = AWSCredentialsFactory.GetAWSCredentials(basicProfile, sharedFile);
                AmazonS3Config amazonS3Config = new AmazonS3Config()
                {
                    RegionEndpoint = basicProfile.Region,
                    ReadWriteTimeout = Timeout
                };

                return new AmazonS3Client(awsCredentials, amazonS3Config);
            }

            return null;
        }

        public static AmazonS3Client GetInstance(RegionEndpoint region)
        {
            CredentialProfile basicProfile;
            SharedCredentialsFile sharedFile = new SharedCredentialsFile();
            if (sharedFile.TryGetProfile(ProfileName, out basicProfile))
            {
                AWSCredentials awsCredentials = AWSCredentialsFactory.GetAWSCredentials(basicProfile, sharedFile);
                return new AmazonS3Client(awsCredentials, region);
            }

            return null;
        }

        public static AmazonS3Client GetInstance(AmazonS3Config config)
        {
            CredentialProfile basicProfile;
            SharedCredentialsFile sharedFile = new SharedCredentialsFile();
            if (sharedFile.TryGetProfile(ProfileName, out basicProfile))
            {
                AWSCredentials awsCredentials = AWSCredentialsFactory.GetAWSCredentials(basicProfile, sharedFile);
                return new AmazonS3Client(awsCredentials, config);
            }

            return null;
        }

        public static AmazonS3Client GetInstance(string bucketName)
        {
            // Bucket Location
            RegionEndpoint regionEndPoint;
            using (IAmazonS3 s3Client = S3Client.GetInstance())
            {
                regionEndPoint = S3Client.GetBucketLocation(s3Client, bucketName);
            }

            // Transfer Acceleration
            using (IAmazonS3 s3Client = S3Client.GetInstance(regionEndPoint))
            {
                BucketAccelerateStatus accelerateStatus = s3Client.GetBucketAccelerateConfiguration(bucketName).Status;
                AmazonS3Config amazonS3Config = new AmazonS3Config()
                {
                    RegionEndpoint = regionEndPoint,
                    UseAccelerateEndpoint = accelerateStatus == BucketAccelerateStatus.Enabled ? true : false,
                    ReadWriteTimeout = Timeout
                };

                return GetInstance(amazonS3Config);
            }
        }

        public static AmazonS3Client GetInstance(string accessKey, string secretAccessKey)
        {
            return GetInstance(accessKey, secretAccessKey, RegionEndpoint.GetBySystemName(DefaultRegion));
            //return GetInstance(accessKey, secretAccessKey, RegionEndpoint.APNortheast1);
            //return GetInstance(accessKey, secretAccessKey, new Uri(ServiceUrl));
        }

        public static AmazonS3Client GetInstance(string accessKey, string secretAccessKey, Uri serviceUrl)
        {
            AmazonS3Config amazonS3Config = new AmazonS3Config()
            {
                ServiceURL = serviceUrl.AbsoluteUri,
                ReadWriteTimeout = Timeout
            };

            return new AmazonS3Client(accessKey, secretAccessKey, amazonS3Config);
        }


        public static AmazonS3Client GetInstance(string accessKey, string secretAccessKey, RegionEndpoint region)
        {
            AmazonS3Config amazonS3Config = new AmazonS3Config()
            {
                RegionEndpoint = region,
                ReadWriteTimeout = Timeout
            };

            return new AmazonS3Client(accessKey, secretAccessKey, amazonS3Config);
        }


        public static AmazonS3Client GetInstance(string accessKey, string secretAccessKey, string bucketName)
        {
            // Bucket Location
            RegionEndpoint regionEndPoint;
            using (IAmazonS3 s3Client = S3Client.GetInstance(accessKey, secretAccessKey))
            {
                regionEndPoint = S3Client.GetBucketLocation(s3Client, bucketName);
            }

            // Transfer Acceleration
            using (IAmazonS3 s3Client = S3Client.GetInstance(accessKey, secretAccessKey, regionEndPoint))
            {
                BucketAccelerateStatus accelerateStatus = s3Client.GetBucketAccelerateConfiguration(bucketName).Status;
                AmazonS3Config amazonS3Config = new AmazonS3Config()
                {
                    RegionEndpoint = regionEndPoint,
                    UseAccelerateEndpoint = accelerateStatus == BucketAccelerateStatus.Enabled ? true : false,
                    ReadWriteTimeout = Timeout
                };

                return new AmazonS3Client(accessKey, secretAccessKey, amazonS3Config);
            }
        }

        public static RegionEndpoint GetBucketLocation(IAmazonS3 s3Client, string bucketName)
        {
            GetBucketLocationRequest getBucketLocationRequest = new GetBucketLocationRequest()
            {
                BucketName = bucketName
            };

            GetBucketLocationResponse getBucketLocationResponse = s3Client.GetBucketLocation(getBucketLocationRequest);
            S3Region s3Region = getBucketLocationResponse.Location;
            return toRegionEndpoint(s3Region);

        }

        private static RegionEndpoint toRegionEndpoint(S3Region region)
        {
            return RegionEndpoint.GetBySystemName(region.Value);
            //if (S3Region.APN1 == region)
            //    return RegionEndpoint.APNortheast1; // Tokyo
            //else if (S3Region.APN2 == region)
            //    return RegionEndpoint.APNortheast2; // Seoul
            //else if (S3Region.APS1 == region)
            //    return RegionEndpoint.APSoutheast1; // Singapore
            //else if (S3Region.APS2 == region)
            //    return RegionEndpoint.APSoutheast2; // Sydney
            //else if (S3Region.EU == region)
            //    return RegionEndpoint.EUWest1;      // Ireland
            //else if (S3Region.EUC1 == region)
            //    return RegionEndpoint.EUCentral1;   // Frankfurt
            //else if (S3Region.SAE1 == region)
            //    return RegionEndpoint.SAEast1;      // Sao Paulo
            //else if (S3Region.US == region)
            //    return RegionEndpoint.USEast1;      // Virginia
            //else if (S3Region.USW1 == region)
            //    return RegionEndpoint.USWest1;      // N.California
            //else if (S3Region.USW2 == region)
            //    return RegionEndpoint.USWest2;      // Oregon
            //else if (S3Region.CN1 == region)
            //    return RegionEndpoint.CNNorth1;     // Beijing
            //else if (S3Region.APS3 == region)
            //    return RegionEndpoint.APSouth1;     // Mumbai
            //throw new ArgumentException("region");
        }
    }
}
