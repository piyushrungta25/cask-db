using Xunit;

namespace cask_db.test;

public class Merge : TestBase
{
    public Merge()
    {
        InitializeDatabase(_softDeleteDataFiles: false, _dataFileSizeThreshold: 32);
    }

    [Fact]
    public void Merge_Basic()
    {
        foreach (int i in Enumerable.Range(0, 5))
            db.Put(Kn(i), Vn(i));

        // should be 3 data files now
        Assert.Equal(3, GetDataFilesCountInDatabase());

        db.Merge();

        // should be 2 data files now, one active file and one merged
        Assert.Equal(2, GetDataFilesCountInDatabase());

        // should be 1 hint file
        Assert.Equal(1, GetHintFilesCountInDatabase());
    }

    [Fact]
    public void Merge_Basic_Persistent()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(i), Vn(i));

        // should be 3 data files now
        Assert.Equal(5, GetDataFilesCountInDatabase());

        db.Merge();
        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        // should be 2 data files now, one active file and one merged
        Assert.Equal(3, GetDataFilesCountInDatabase());

        // should be 1 hint file
        Assert.Equal(1, GetHintFilesCountInDatabase());
    }

    [Fact]
    public void Merge_Operations()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(1), Vn(i));

        db.Merge();

        Assert.Equal(Vn(8), db.Get(Kn(1)));
    }

    [Fact]
    public void Merge_Operations_Persistent()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(1), Vn(i));

        db.Merge();
        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(Vn(8), db.Get(Kn(1)));
    }

    [Fact]
    public void Merge_Operations_1()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(1), Vn(i));

        db.Merge();

        db.Put(Kn(1), Vn(42));

        Assert.Equal(Vn(42), db.Get(Kn(1)));
    }

    [Fact]
    public void Merge_Operations_1_Persistent()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(1), Vn(i));

        db.Merge();
        db.Put(Kn(1), Vn(42));

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(Vn(42), db.Get(Kn(1)));
    }

    [Fact]
    public void Merge_Operations_2()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(1), Vn(i));

        db.Merge();

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        db.Put(Kn(1), Vn(42));

        Assert.Equal(Vn(42), db.Get(Kn(1)));
    }

    [Fact]
    public void Merge_Operations_Delete()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(i), Vn(i));

        foreach (int i in Enumerable.Range(0, 8))
            db.Delete(Kn(i));

        db.Merge();

        Assert.Equal(Vn(8), db.Get(Kn(8)));

        foreach (int i in Enumerable.Range(0, 8))
            Assert.Null(db.Get(Kn(i)));
    }

    [Fact]
    public void Merge_Operations_Delete_Persistent()
    {
        foreach (int i in Enumerable.Range(0, 9))
            db.Put(Kn(i), Vn(i));

        foreach (int i in Enumerable.Range(0, 8))
            db.Delete(Kn(i));

        db.Merge();
        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(Vn(8), db.Get(Kn(8)));

        foreach (int i in Enumerable.Range(0, 8))
            Assert.Null(db.Get(Kn(i)));
    }

    [Fact]
    public void Merge_FileSizes()
    {
        // each record is 16 bytes, total 256
        foreach (int i in Enumerable.Range(0, 16))
            db.Put(Kn(1), Vn(i));

        db.Sync();
        Assert.Equal(256, GetDatabaseSize());

        db.Merge();

        Assert.Equal(Vn(15), db.Get(Kn(1)));
        Assert.Equal(16, GetDatabaseSize());

        db.Delete(Kn(1));
        db.Merge();

        Assert.Equal(0, GetDatabaseSize());
    }

    [Fact]
    public void Merge_FileSizes_Persistent()
    {
        // each record is 16 bytes, total 256
        foreach (int i in Enumerable.Range(0, 16))
            db.Put(Kn(1), Vn(i));

        db.Sync();
        Assert.Equal(256, GetDatabaseSize());

        db.Merge();
        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(Vn(15), db.Get(Kn(1)));
        Assert.Equal(16, GetDatabaseSize());

        db.Delete(Kn(1));
        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });
        db.Merge();

        db.Dispose();
        db = new CaskDB(new CaskDbOpts
        {
            DatabaseDirectory = DatabaseDirectory,
        });

        Assert.Equal(0, GetDatabaseSize());
    }
}
