using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Newtonsoft.Json.Linq;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LambdaToS3
{
    public class LambdaToS3Main
    {

        private readonly string _accessKey;
        private readonly string _secretKey;
        private readonly string _bucketName;
        private readonly RegionEndpoint _bucketRegion;
        private const string LocalTempFileName = "temp_download";
        private const int NumTests = 3;

        public LambdaToS3Main()
        {
            // deployment-specific details that should not be committed to version control
            _accessKey = Environment.GetEnvironmentVariable("AccessKey");
            _secretKey = Environment.GetEnvironmentVariable("SecretKey");
            _bucketName = Environment.GetEnvironmentVariable("BucketName");
            _bucketRegion = RegionEndpoint.GetBySystemName(
                                Environment.GetEnvironmentVariable("BucketRegion"));
        }

        public string FunctionHandler(JObject input, ILambdaContext context)
        {
            return UploadAsync(input, context.RemainingTime).Result;
        }

        private async Task<string> UploadAsync(JObject input, TimeSpan contextRemainingTime)
        {
            var client = new AmazonS3Client(_accessKey, _secretKey, _bucketRegion);

            string bucketName = $"{_bucketName}/{input["remoteFileDir"]}";
            string remoteSourceFileName = input["remoteSourceFileName"].ToString();
            string remoteTargetFileName = input["remoteTargetFileName"].ToString();
            long partSize = long.Parse(input["partSize"].ToString());
            if (remoteSourceFileName == remoteTargetFileName)
                throw new ArgumentException("Please provide the different file for uploading");
            string localFilePath = $"/tmp/{LocalTempFileName}";
            dynamic results = new JObject();

            //Download the file first
            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = remoteSourceFileName
            };

            var sw = new Stopwatch();
            var getResponse = client.GetObjectAsync(getRequest);
            sw.Start();
            await getResponse.Result.WriteResponseStreamToFileAsync(localFilePath, false, new CancellationToken());
            sw.Stop();
            results.DownloadTime  = sw.ElapsedMilliseconds;

            //Then upload it with a different name
            
            //1. use put request
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = remoteTargetFileName,
                FilePath = localFilePath
            };
            //warm up
            await client.PutObjectAsync(putRequest);
            sw.Restart();
            for(var i = 0; i < NumTests; i++)
                await client.PutObjectAsync(putRequest);
            sw.Stop();
            results.PutUploadTime = sw.ElapsedMilliseconds/NumTests;


            //2. use transfer utility
            var transferUtility = new TransferUtility(client);

            //2a. transfer file
            var transferUtilityRequest = new TransferUtilityUploadRequest
            {
                BucketName = bucketName,
                FilePath = localFilePath,
                Key = remoteTargetFileName,
                PartSize = partSize
            };
            //warm up
            transferUtility.Upload(transferUtilityRequest);
            //await transferUtility.UploadAsync(transferUtilityRequest);
            sw.Restart();
            for(var i = 0; i < NumTests; i++)
                transferUtility.Upload(transferUtilityRequest);
                //await transferUtility.UploadAsync(transferUtilityRequest);
            sw.Stop();
            results.PartSize = partSize;
            results.FileUploadTime = sw.ElapsedMilliseconds/NumTests;

            //2b. upload file stream direclty
            //warm up
            transferUtility.Upload(new FileStream(localFilePath, FileMode.Open), bucketName, remoteTargetFileName);
            sw.Restart();
            for(var i = 0; i < NumTests; i++)
                transferUtility.Upload(new FileStream(localFilePath, FileMode.Open), bucketName, remoteTargetFileName);
            sw.Stop();
            results.StreamUploadTime = sw.ElapsedMilliseconds/NumTests;

            results.RemainingTime = contextRemainingTime.ToString();

            return results.ToString();
        }
    }

}
