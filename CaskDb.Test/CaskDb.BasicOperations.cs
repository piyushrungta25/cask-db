namespace cask_db.test;

using cask_db;
using Xunit;

public class BasicOperations : TestBase
{
    [Fact]
    public void ReadAndWrite()
    {
        db.Put("k1", "v1");
        Assert.Equal("v1", db.Get("k1"));

        db.Put("k1", "v2");
        Assert.Equal("v2", db.Get("k1"));

        db.Put("k2", "v3");
        Assert.Equal("v3", db.Get("k2"));
        Assert.Equal("v2", db.Get("k1"));
    }

    [Fact]
    public void ReadAndWritePersistant_1()
    {
        db.Put("k1", "v1");
        db.Put("k1", "v2");
        db.Put("k2", "v3");

        Assert.Equal("v2", db.Get("k1"));
        Assert.Equal("v3", db.Get("k2"));

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });
        Assert.Equal("v2", db.Get("k1"));
        Assert.Equal("v3", db.Get("k2"));
    }

    [Fact]
    public void Delete()
    {
        Assert.Null(db.Get("k1"));
        db.Put("k1", "v1");
        Assert.Equal("v1", db.Get("k1"));

        db.Delete("k1");
        Assert.Null(db.Get("k1"));
    }

    [Fact]
    public void Delete_Persistent_1()
    {
        db.Put("k1", "v1");
        Assert.Equal("v1", db.Get("k1"));
        db.Delete("k1");
        Assert.Null(db.Get("k1"));

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Null(db.Get("k1"));
    }

    [Fact]
    public void Delete_Persistent_2()
    {
        db.Put("k1", "v1");
        Assert.Equal("v1", db.Get("k1"));
        db.Delete("k1");
        Assert.Null(db.Get("k1"));
        db.Put("k1", "v2");

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal("v2", db.Get("k1"));
    }
}
