using System.Diagnostics;
using Serilog;

namespace cask_db;

public class StopwatchLog : IDisposable
{
    ILogger logger = Log.ForContext<StopwatchLog>();

    private Stopwatch _stopwatch = new Stopwatch();
    private Action<TimeSpan>? _callback;

    public StopwatchLog() { }

    public StopwatchLog(string name)
    {
        _callback = (ts) =>
        {
            Console.WriteLine("{0, 48}: {1} ms", name, ts.TotalMilliseconds);
        };

        _stopwatch.Start();
    }

    public StopwatchLog(Action<TimeSpan> callback)
        : this()
    {
        _callback = callback;
    }

    public static StopwatchLog Start(Action<TimeSpan> callback)
    {
        return new StopwatchLog(callback);
    }

    public void Dispose()
    {
        _stopwatch.Stop();
        if (_callback != null)
            _callback(Result);
    }

    public TimeSpan Result
    {
        get { return _stopwatch.Elapsed; }
    }
}
