using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GOO.Net.Framework.Azure.Blobs
{
    public interface IApplication
    {
        Task RunAsync();
    }

    public class Application : IApplication
    {
        private readonly IBlobRepo _blobRepo;

        public Application()
        {
            _blobRepo = new AzureBlobRepo();
        }

        public async Task RunAsync()
        {
            var random = Guid.NewGuid().ToString();
            var path = $"inbound{random}.txt";
            await _blobRepo.SaveAsync("messages", path, random);

            var savedText = await _blobRepo.ReadTextAsync("messages", path);

            Console.WriteLine($"messages {path}");
            Console.WriteLine(savedText);
        }
    }
}
