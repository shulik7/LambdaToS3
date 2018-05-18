using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LambdaToS3
{
    public class LambdaToS3Main
    {

        private readonly string _accessKey;
        private readonly string _secretKey;
        //private readonly string _downloadBucketName;
        private readonly string _uploadBucketName;
        private readonly RegionEndpoint _bucketRegion;
        private const string LocalTempFileName = "temp_download";

        public LambdaToS3Main()
        {
            // deployment-specific details that should not be committed to version control
            _accessKey = Environment.GetEnvironmentVariable("AccessKey");
            _secretKey = Environment.GetEnvironmentVariable("SecretKey");
            //_downloadBucketName = Environment.GetEnvironmentVariable("DownloadBucketName");
            _uploadBucketName = Environment.GetEnvironmentVariable("UploadBucketName");
            _bucketRegion = RegionEndpoint.GetBySystemName(
                                Environment.GetEnvironmentVariable("BucketRegion"));
        }

        public LambdaOutput FunctionHandler(JObject input, ILambdaContext context)
        {
            return UploadAsync(input, context.RemainingTime).Result;
        }


        private async Task<LambdaOutput> UploadAsync(JObject input, TimeSpan contextRemainingTime)
        {
            var client = new AmazonS3Client(_accessKey, _secretKey, _bucketRegion);

            string bucketName = $"{_uploadBucketName}/{input["remoteFileDir"]}";
            string remoteSourceFileName = input["remoteSourceFileName"].ToString();
            string remoteTargetFileName = input["remoteTargetFileName"].ToString();
            if (remoteSourceFileName == remoteTargetFileName)
                throw new ArgumentException("Please provide the different file for uploading");
            string localFilePath = $"/tmp/{LocalTempFileName}";

            //Download the file first
            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = remoteSourceFileName
            };

            var sw = new Stopwatch();
            sw.Start();
            var getResponse = client.GetObjectAsync(getRequest);
            await getResponse.Result.WriteResponseStreamToFileAsync(localFilePath, false, new CancellationToken());
            sw.Stop();
            var downloadTime = sw.ElapsedMilliseconds;

            //Then upload it with a different name
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = remoteTargetFileName,
                FilePath = localFilePath
            };

            sw.Restart();
            await client.PutObjectAsync(putRequest);
            sw.Stop();
            var uploadTime = sw.ElapsedMilliseconds;
            

            //return response.Result.ResponseMetadata.ToString();
            return new LambdaOutput
            {
                RemainingTime = contextRemainingTime.ToString(),
                DownloadTime = downloadTime,
                UploadTime = uploadTime
            };
        }
    }

}
