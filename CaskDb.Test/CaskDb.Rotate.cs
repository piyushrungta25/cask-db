using Xunit;

namespace cask_db.test;

public class Rotate : TestBase
{
    [Fact]
    public void RotateDataFile_AutoRotate_Basic()
    {
        foreach (int i in Enumerable.Range(0, (4096 * 2) - 1))
        {
            db.Put(new string('0', 88), new string('1', 4000));
        }

        db.Dispose();

        Assert.Equal(2, GetDataFilesCountInDatabase());
    }

    [Fact]
    public void RotateDataFile_AutoRotate_Basics()
    {
        InitializeDatabase(_dataFileSizeThreshold: 32);

        db.Put(Kn(1), Vn(1));
        db.Put(Kn(2), Vn(2));

        // file should be rotate here

        db.Put(Kn(1), Vn(3));
        db.Delete(Kn(2));

        Assert.Equal(2, GetDataFilesCountInDatabase());

        Assert.Equal(Vn(3), db.Get(Kn(1)));
        Assert.Null(db.Get(Kn(2)));
    }

    [Fact]
    public void RotateDataFile_AutoRotate_Persistent()
    {
        InitializeDatabase(_dataFileSizeThreshold: 32);

        db.Put(Kn(1), Vn(1));
        db.Put(Kn(2), Vn(2));

        // file should be rotate here

        db.Put(Kn(1), Vn(3));
        db.Delete(Kn(2));

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(3, GetDataFilesCountInDatabase());

        Assert.Equal(Vn(3), db.Get(Kn(1)));
        Assert.Null(db.Get(Kn(2)));
    }

    [Fact]
    public void RotateDataFile_FilesOnDisk()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");
        db.Put("k1", "v4");

        db.Dispose();

        Assert.Equal(2, GetDataFilesCountInDatabase());
    }

    [Fact]
    public void RotateDataFile_Basic()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");

        Assert.Equal("v1", db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }

    [Fact]
    public void RotateDataFile_Update()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");
        db.Put("k1", "v4");

        Assert.Equal("v4", db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }

    [Fact]
    public void RotateDataFile_Delete()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");
        db.Delete("k1");

        Assert.Null(db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }

    [Fact]
    public void RotateDataFile_Basic_Persistant()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal("v1", db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }

    [Fact]
    public void RotateDataFile_Update_Persistant()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");
        db.Put("k1", "v4");

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal("v4", db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }

    [Fact]
    public void RotateDataFile_Delete_Persistant()
    {
        db.Put("k1", "v1");
        db.Put("k2", "v2");
        db.RotateDataFile();
        db.Put("k3", "v3");
        db.Delete("k1");

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Null(db.Get("k1"));
        Assert.Equal("v2", db.Get("k2"));
        Assert.Equal("v3", db.Get("k3"));
    }
}
