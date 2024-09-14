using System.Diagnostics.CodeAnalysis;

namespace cask_db.test;

public abstract class TestBase : IDisposable
{
    protected string DatabaseDirectory;
    protected CaskDB db;

    public TestBase()
    {
        InitializeDatabase();
    }

    [MemberNotNull(nameof(db))]
    [MemberNotNull(nameof(DatabaseDirectory))]
    protected void InitializeDatabase(
        string? _DatabaseDirectory = null,
        int _dataFileSizeThreshold = 16 * 1024 * 1024,
        bool _softDeleteDataFiles = true
    )
    {
        DatabaseDirectory = _DatabaseDirectory ?? CreateTempDirectory();
        
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
            DataFileSizeThresholdInBytes = _dataFileSizeThreshold,
            SoftDeleteDataFiles = _softDeleteDataFiles
        });
    }

    public void Dispose()
    {
        db.Dispose();
    }

    protected string Kn(int n)
    {
        return "k" + n.ToString("000");
    }

    protected string Vn(int n)
    {
        return n.ToString("0000");
    }

    protected IEnumerable<string> GetDataFiles()
    {
        foreach (string file in Directory.GetFiles(DatabaseDirectory, "*.data"))
            yield return file;
    }

    protected IEnumerable<long> GetDataFileSizes()
    {
        foreach (string file in GetDataFiles())
        {
            yield return new FileInfo(file).Length;
        }
    }

    protected long GetDatabaseSize()
    {
        return GetDataFileSizes().Sum();
    }

    protected int GetDataFilesCountInDatabase()
    {
        return GetDataFiles().Count();
    }

    protected int GetHintFilesCountInDatabase()
    {
        var dataFiles = Directory.GetFiles(DatabaseDirectory, "*.hint");
        return dataFiles.Length;
    }

    private string CreateTempDirectory()
    {
        var tempDirectoryName =
            "CaskDb." + Path.GetFileNameWithoutExtension(Path.GetRandomFileName());
        var tempDirectory = Path.Combine(Path.GetTempPath(), tempDirectoryName);

        Directory.CreateDirectory(tempDirectory);

        return tempDirectory;
    }
}
