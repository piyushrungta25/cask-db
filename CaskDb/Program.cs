using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Serilog;

namespace cask_db;

class Program
{
    private static Random rand = new Random();

    private static ILogger logger = Log.ForContext<Program>();

    private static long PeakMemory;

    static void Main(string[] args)
    {
        // Initialize logger
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Error()
            .Enrich.WithThreadId()
            .WriteTo.Console()
            .WriteTo.File(
                $"logs/{DateTime.UtcNow:yyyy-MM-dd--HH-mm-ss-ffff}.log",
                outputTemplate: "{Timestamp:u} [{Level:u3}][{SourceContext}][Tid:{ThreadId}] {Message:lj}{NewLine}{Exception}"
            )
            .CreateLogger();

        try
        {
            Task.Run(async () =>
            {
                await Task.Delay(100);
                PeakMemory = Math.Max(PeakMemory, Process.GetCurrentProcess().WorkingSet64);
            });

            var dbDir = $"cdb/{DateTime.UtcNow:yyyy-MM-dd--HH-mm-ss-ffff}";
            WriteOperations(dbDir);
            WriteOperationsSync(dbDir);
            ReadOperations(dbDir);
            Console.WriteLine("{0, 48}: {1} Mb", "Peak memory", PeakMemory / 1024.0 / 1024);
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    private static void WriteOperations(string dbDir)
    {
        var keyCount = 1_000_000;
        var guidList = Enumerable
            .Range(0, keyCount)
            .Select(_ => Guid.NewGuid().ToString("N"))
            .ToArray();
        var keylist = Enumerable.Range(0, keyCount).Select(x => (x % 10000).ToString()).ToArray();

        var cdb = new CaskDB(
            new CaskDbOpts
            {
                DatabaseDirectory = dbDir,
                DataFileSizeThresholdInBytes = 5 * 1024 * 1024
            }
        );

        using (var _ = new StopwatchLog("write async c=1M"))
        {
            for (var i = 0; i < keylist.Length; i++)
                cdb.Put(keylist[i], guidList[i]);
        }
        cdb.Merge();
        cdb.SyncAndClose();
    }

    private static void WriteOperationsSync(string dbDir)
    {
        var keyCount = 1_000_000;
        var guidList = Enumerable
            .Range(0, keyCount)
            .Select(_ => Guid.NewGuid().ToString("N"))
            .ToArray();
        var keylist = Enumerable.Range(0, keyCount).Select(x => (x % 10000).ToString()).ToArray();

        var cdb = new CaskDB(
            new CaskDbOpts
            {
                DatabaseDirectory = dbDir,
                DataFileSizeThresholdInBytes = 5 * 1024 * 1024,
                UseSynchronousWrites = true
            }
        );

        using (var _ = new StopwatchLog("write sync c=1M"))
        {
            for (var i = 0; i < keylist.Length; i++)
                cdb.Put(keylist[i], guidList[i]);
        }
        cdb.Merge();
        cdb.SyncAndClose();
    }

    private static void ReadOperations(string dbDir)
    {
        var keyCount = 1_000_000;
        var keylist = Enumerable.Range(0, keyCount).Select(x => (x % 10000).ToString()).ToArray();

        var cdb = new CaskDB(
            new CaskDbOpts
            {
                DatabaseDirectory = dbDir,
                DataFileSizeThresholdInBytes = 5 * 1024 * 1024
            }
        );

        using (var _ = new StopwatchLog("read c=1M"))
        {
            for (var i = 0; i < keylist.Length; i++)
                cdb.Get(keylist[i]);
        }

        cdb.SyncAndClose();
    }
}
