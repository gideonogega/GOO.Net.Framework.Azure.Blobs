using System.Collections.Generic;
using System.Threading.Tasks;

namespace GOO.Net.Framework.Azure.Blobs
{
    public interface IBlobRepo
    {
        Task SaveAsync(string containerName, string path, string text, Dictionary<string, string> metadata = null);
        Task<string> ReadTextAsync(string containerName, string path);
        Task<string> ReadTextOrNullAsync(string containerName, string path);
        Task<List<string>> ReadAllTextAsync(string containerName, string directory, string prefix = null);
        Task<List<string>> ListBlobPathsAsync(string containerName, string directory, string prefix = null);
        Task<bool> DeleteAsync(string containerName, string path);
        Task<bool> DeleteContainerAsync(string containerName);
        Task<List<string>> ListContainerNames();
    }
}
