using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace GOO.Net.Framework.Azure.Blobs
{
    public class AzureBlobRepo : IBlobRepo
    {
        private static readonly ConcurrentBag<string> ContainersCreated = new ConcurrentBag<string>();

        private readonly string _connectionString;

        public AzureBlobRepo()
        {
            _connectionString = ConfigurationManager.AppSettings["AzureBlobStorage"];

            Console.WriteLine($"Connection String: {_connectionString}");
        }

        public async Task SaveAsync(string containerName, string path, string text, Dictionary<string, string> metadata = null)
        {
            var blockBlob = await GetOrCreateBlobAsync(containerName, path);
            await SetBlobMetadataAsync(blockBlob, metadata);
            await blockBlob.UploadTextAsync(text);
            Console.WriteLine($"AzureBlobRepo.SaveAsync {containerName} {path} complete.");
        }

        public async Task<string> ReadTextAsync(string containerName, string path)
        {
            var blockBlob = await GetOrCreateBlobAsync(containerName, path);
            return await GetBlobTextWithRetry(blockBlob);
        }

        public async Task<string> ReadTextOrNullAsync(string containerName, string path)
        {
            try
            {
                return await ReadTextAsync(containerName, path);
            }
            catch (StorageException ex)
            {
                Console.WriteLine($"Unable to read {path}: {ex.Message}");
            }

            return null;
        }

        public async Task<List<string>> ReadAllTextAsync(string containerName, string directory, string prefix = null)
        {
            var texts = new List<string>();

            var container = await GetOrCreateContainerAsync(containerName);

            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = await container.ListBlobsSegmentedAsync(directory, true, BlobListingDetails.None, null, blobContinuationToken, null, null);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                foreach (IListBlobItem item in results.Results)
                {
                    if (!(item is CloudBlockBlob blockBlob)
                        || !MatchesPrefix(blockBlob, directory, prefix)) continue;

                    texts.Add(await blockBlob.DownloadTextAsync());
                }
            } while (blobContinuationToken != null);

            return texts;
        }

        public async Task<List<string>> ListBlobPathsAsync(string containerName, string directory, string prefix = null)
        {
            var paths = new List<string>();

            var container = await GetOrCreateContainerAsync(containerName);

            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = await container.ListBlobsSegmentedAsync(directory, true, BlobListingDetails.None, null, blobContinuationToken, null, null);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                foreach (IListBlobItem item in results.Results)
                {
                    if (!(item is CloudBlockBlob blockBlob)
                        || !MatchesPrefix(blockBlob, directory, prefix)) continue;

                    paths.Add(blockBlob.Name);
                }
            } while (blobContinuationToken != null);

            return paths;
        }

        public async Task<bool> DeleteAsync(string containerName, string path)
        {
            var blockBlob = await GetOrCreateBlobAsync(containerName, path);
            return await blockBlob.DeleteIfExistsAsync();
        }

        public async Task<List<string>> ListContainerNames()
        {
            var names = new List<string>();

            var storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = await blobClient.ListContainersSegmentedAsync(blobContinuationToken);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                foreach (var container in results.Results)
                {
                    names.Add(container.Name);
                }
            } while (blobContinuationToken != null);

            return names;
        }

        public async Task<bool> DeleteContainerAsync(string containerName)
        {
            var container = GetContainer(containerName);
            return await container.DeleteIfExistsAsync();
        }

        private async Task<CloudBlobContainer> GetOrCreateContainerAsync(string containerName)
        {
            var container = GetContainer(containerName);

            // Once a container is created it will remain forever.
            // So here we avoid an extra Azure service call if we've already
            // set up the container.
            // Also, this avoids misleading errors in Application Insights
            // which occur if CreateIfNotExistsAsync is called on a container
            // that already exists
            if (!ContainersCreated.Contains(containerName))
            {
                await container.CreateIfNotExistsAsync();
                ContainersCreated.Add(containerName);
            }

            return container;
        }

        private CloudBlobContainer GetContainer(string containerName)
        {
            var storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(containerName.ToLower());
        }

        private async Task<CloudBlockBlob> GetOrCreateBlobAsync(string containerName, string blobName)
        {
            var container = await GetOrCreateContainerAsync(containerName);
            var blockBlob = container.GetBlockBlobReference(blobName);

            return blockBlob;
        }

        private static async Task SetBlobMetadataAsync(CloudBlockBlob blockBlob, Dictionary<string, string> metadata)
        {
            if (metadata != null)
            {
                foreach (var kv in metadata)
                {
                    blockBlob.Metadata.Add(kv.Key, kv.Value);
                }
                await blockBlob.SetMetadataAsync();
            }
        }

        private bool MatchesPrefix(CloudBlockBlob blockBlob, string directory, string prefix = null)
        {
            if (prefix == null) return true;

            var blobPrefix = $"{directory}/{prefix}";

            return blockBlob.Name.StartsWith(blobPrefix, StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary>
        /// Logs show that occasionally when blob is written and then immediately read, the read
        /// can fail with a System.Net.WebException with message "The remove server returned an error:
        /// (404) Not Found."
        /// It is a good practice to retry all Azure resource calls.
        /// </summary>
        private async Task<string> GetBlobTextWithRetry(CloudBlockBlob blockBlob)
        {
            // This could be improved with a general-purpose Retry tool with
            // exponential backoff, and controlled by config settings

            // First try
            try
            {
                return await blockBlob.DownloadTextAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"First attempt to get blob text resulted in {e.GetType().Name}: {e.Message}. Retrying...");
            }

            // Second try (but Wait a while first). This time let the call fail.
            await Task.Delay(1000);
            return await blockBlob.DownloadTextAsync();
        }
    }
}
