# s3hash-net
.net Compute Hash for S3

## Setting
priority of credentials
1. AwsS3Hash.Net45.exe --accesskey XXXXXXXX --secretkey YYYYYYY
2. ~/.aws/credentials
3. EC2 IAM Role

```
## AwsS3Hash.Net45.exe --help
Usage: AwsS3Hash.Net45 [options]

Options:
  -a, --accesskey=VALUE      AWS access key.
  -s, --secretkey=VALUE      AWS secret access key.
  -r, --region=VALUE         AWS region. default ap-northeast-1
  -p, --path=VALUE           s3 file path. ex s3://bucket/key
      --type=VALUE           compute hash type [MD5|SHA1|SHA256|SHA384|SHA512].
                               default SHA1
      --part=VALUE           part size(MB) 5 to 100 (maximum 100MB). default 5
      --parallel=VALUE       parallel download count 1 to 64. default 5
  -t, --timeout=VALUE        timeout(sec). default 300
  -l, --log=VALUE            log file.
  -?, --help                 show help.
```
## Usage example
```
$ AwsS3Hash.Net45.exe --accesskey XXXXXXXX --secretkey YYYYYYY --path s3://bucket/key
$ AwsS3Hash.Net45.exe -a XXXXXXXX -s YYYYYYY -p s3://bucket/key
$ AwsS3Hash.Net45.exe -a XXXXXXXX -s YYYYYYY -p s3://bucket/key --type MD5
$ AwsS3Hash.Net45.exe -a XXXXXXXX -s YYYYYYY -p s3://bucket/key --type MD5 --log trace.log
$ AwsS3Hash.Net45.exe -p s3://bucket/key
$ AwsS3Hash.Net45.exe -p s3://bucket/key --type MD5
$ AwsS3Hash.Net45.exe -p s3://bucket/key --part 10 --parallel 3
```


