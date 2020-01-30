using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace GOO.Net.Framework.Azure.Blobs
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                IApplication application = new Application();
                application.RunAsync().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }

            Console.WriteLine("Press any key to continue.");
            Console.ReadKey();
        }
    }
}
