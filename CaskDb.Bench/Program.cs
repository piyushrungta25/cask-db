using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace CaskDb.Bench;

// [ShortRunJob]
[MemoryDiagnoser]
public class MemoryBenchmarkerDemo
{
    private byte[] span;
    private FileStream fs1;
    private FileStream fs2;
    private byte[] buffer;
    public string[] Guids { get; set; }


    [GlobalSetup]
    public void Setup()
    {
        span = new byte[1024];
        this.buffer = new byte[32];
        
        this.Guids = Enumerable.Range(0, 1000).Select(i => Guid.NewGuid().ToString("N")).ToArray();
        
        this.fs1 = File.Open(
            "fs1",
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read
        );
        
        this.fs2 = File.Open(
            "fs2",
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read
        );
    }


    // [Benchmark]
    public void FileAppend()
    {
        using (var stream = new FileStream("fs1", FileMode.Append))
        {
            stream.Write(new Span<byte>(span));
        }
    }

    // this order of magnitude better
    // [Benchmark]
    public void FileWrite()
    {
            fs1.Write(new Span<byte>(span));
    }
    
    [Benchmark]
    public void FileWrite_WithAllocation()
    {
        foreach (var guid in Guids)
        {
            fs1.Write(Encoding.UTF8.GetBytes(guid));
        }
    }
    
    // slightly better
    [Benchmark]
    public void FileWrite_WithoutAllocation()
    {
        foreach (var guid in Guids)
        {
            Encoding.UTF8.GetBytes(guid, buffer);
            fs2.Write(buffer);
        }
    }

}


class Program
{
    static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<MemoryBenchmarkerDemo>();
    }
}