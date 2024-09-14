using System.Diagnostics.CodeAnalysis;
using Serilog;

namespace cask_db;

public class CaskDB : IDisposable
{
    ILogger logger = Log.ForContext<CaskDB>();

    FileOps FileOps;

    string ActiveFileName;

    FileStream ActiveFileStream;
    FileStream? MergeFileStream;

    List<string> DataFiles;
    Dictionary<string, FileStream> ReadFileHandles;
    KeyDir KeyDir;
    int BytesWrittenSinceLastRotate;

    // Using a dedicated object for locking instead of just ActiveFileStream since
    // we are modifying ActiveFileStream from within the lock
    // This is not safe, after the ActiveFileStream is overwritten
    // any new code path trying to lock on the active file stream will get the lock
    // violating our guarantee that the active file is not written to from withint the lock body
    object FileWriteLock = new object();

    object FileMergeLock = new object();

    LockManager LockManager;
    private readonly CaskDbOpts opts;

    public CaskDB(CaskDbOpts opts)
    {
        this.opts = opts;
        InitializeDataDirectory(opts.DatabaseDirectory);

        ReadFileHandles = new();
        FileOps = new();
        LockManager = new();

        BytesWrittenSinceLastRotate = 0;

        InitializeKeyDir(DataFiles);

        InitializeActiveFile();
    }

    [MemberNotNull(nameof(DataFiles))]
    private void InitializeDataDirectory(string _DatabaseDirectory)
    {
        opts.DatabaseDirectory = Path.GetFullPath(_DatabaseDirectory);
        Directory.CreateDirectory(opts.DatabaseDirectory);
        DataFiles = Directory.EnumerateFiles(opts.DatabaseDirectory, "*.data").ToList();
        DataFiles.Sort();

        logger.Information(
            "Found {count} files in {databaseDir}: {@datafiles}",
            DataFiles.Count(),
            opts.DatabaseDirectory,
            DataFiles
        );
    }

    [MemberNotNull(nameof(KeyDir))]
    private void InitializeKeyDir(List<string> dataFiles)
    {
        KeyDir = new KeyDir();
        foreach (var file in dataFiles)
        {
            if (HintFileExists(file, out string hintfile))
            {
                FileStream fs = GetReadFileStream(hintfile);
                KeyDir.InitializeWithHintFile(fs, file);
            }
            else
            {
                FileStream fs = GetReadFileStream(file);
                KeyDir.InitializeWithDataFile(fs);
            }
        }
    }

    [MemberNotNull(nameof(ActiveFileName))]
    [MemberNotNull(nameof(ActiveFileStream))]
    private void InitializeActiveFile()
    {
        string? latestDataFile = GetLatestDataFile(DataFiles);
        int activeFileIndex = 1;
        if (latestDataFile != null)
        {
            var dataFileIndex = GetFileIndex(latestDataFile);
            activeFileIndex = dataFileIndex + 1;
        }

        ActiveFileName = activeFileIndex.ToString("000000000000") + ".data";
        ActiveFileName = Path.Join(opts.DatabaseDirectory, ActiveFileName);

        logger.Information($"new active filename {ActiveFileName}");

        ActiveFileStream = File.Open(
            ActiveFileName,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read
        );
    }

    private bool HintFileExists(string file, out string hintfile)
    {
        hintfile = GetHintFileNameFromDataFileName(file);
        return Path.Exists(hintfile);
    }

    public string GetHintFileNameFromDataFileName(string dataFile)
    {
        string hintFileName = Path.GetFileNameWithoutExtension(dataFile) + ".hint";
        hintFileName = Path.Join(opts.DatabaseDirectory, hintFileName);
        return hintFileName;
    }

    private int GetFileIndex(string latestDataFile)
    {
        var fileName = Path.GetFileNameWithoutExtension(latestDataFile);
        return int.Parse(fileName);
    }

    private string? GetLatestDataFile(List<string> dataFiles)
    {
        return dataFiles.LastOrDefault();
    }

    private void RemoveDataFile(string dataFile)
    {
        this.DataFiles.Remove(dataFile);

        if (opts.SoftDeleteDataFiles)
            File.Move(dataFile, dataFile + ".deleted", false);
        else
            File.Delete(dataFile);
    }

    private FileStream GetReadFileStream(string file)
    {
        if (!ReadFileHandles.TryGetValue(file, out var fs))
        {
            fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            ReadFileHandles[file] = fs;
        }

        return fs;
    }

    public void DumpKeyDir()
    {
        logger.Debug("KeyDir state: {@keyDir}", this.KeyDir);
    }

    public string? Get(string key)
    {
        if (!KeyDir.TryGetValue(key, out var val))
            return null;

        logger.Debug("Found {key} = {@val} in keydir", key, val);

        var readFileStream = GetReadFileStream(val.FileId ?? throw new ArgumentException());

        // if the read key is from the current active file,
        // the value might not have been flused to the disk yet
        if (readFileStream.Name == ActiveFileStream.Name)
            lock (FileWriteLock)
                Sync();

        if (MergeFileStream != null && MergeFileStream.Name == readFileStream.Name)
            lock (FileMergeLock)
                MergeFileStream.Flush();

        // reading seeks the file
        lock (readFileStream)
        {
            return FileOps.ReadValueAtPosition(readFileStream, val.ValueSize, val.ValuePosition);
        }
    }

    public void Put(string key, string? val)
    {
        lock (LockManager.GetLockObject(key))
        {
            WriteResult result;
            lock (FileWriteLock)
            {
                // write to data file
                result = FileOps.WriteRecordSingleBuffer(ActiveFileStream, key, val);
                BytesWrittenSinceLastRotate += result.BytesWritten;

                if (opts.UseSynchronousWrites)
                    Sync();

                if (BytesWrittenSinceLastRotate >= opts.DataFileSizeThresholdInBytes)
                {
                    logger.Debug(
                        "Auto rotated data file, bytesWrittenSinceLastRotate: {}",
                        BytesWrittenSinceLastRotate
                    );
                    RotateDataFile();
                }
            }

            // update the Keydir
            if (val == null)
                KeyDir.Remove(key);
            else
                KeyDir.Insert(key, result.FileValue);
        }
    }

    public void Delete(string key)
    {
        this.Put(key, null);
    }

    public void RotateDataFile()
    {
        lock (FileWriteLock)
        {
            // add current file to the data file list
            this.DataFiles.Add(ActiveFileName);
            this.DataFiles.Sort();

            // flush and close the current file
            SyncAndClose();

            // create a new active file and update the active file handle
            InitializeActiveFile();

            BytesWrittenSinceLastRotate = 0;
        }
    }

    public void Merge()
    {
        FileStream hintFileStream;
        List<string> DataFilesToMerge;
        string mergeFile;

        lock (FileWriteLock)
        {
            RotateDataFile();

            DataFilesToMerge = this.DataFiles.ToList();

            mergeFile = ActiveFileName;
            RotateDataFile();
        }

        string hintFileName = GetHintFileNameFromDataFileName(mergeFile);
        MergeFileStream = File.Open(mergeFile, FileMode.Append, FileAccess.Write, FileShare.Read);
        hintFileStream = File.Open(hintFileName, FileMode.Append, FileAccess.Write);

        foreach (var dataFile in DataFilesToMerge)
        {
            // don't use cached file handles since we want to sequentially read the full file
            var dataFileStream = File.Open(
                dataFile,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite
            );
            TryMergeSingleFile(dataFileStream, MergeFileStream, hintFileStream);

            dataFileStream.Close();
            hintFileStream.Flush();

            // merge successful
            RemoveDataFile(dataFile);
        }

        hintFileStream.Flush();
        hintFileStream.Close();

        lock (FileMergeLock)
        {
            MergeFileStream.Flush();
            MergeFileStream.Close();
            MergeFileStream = null;
        }
    }

    private void TryMergeSingleFile(
        FileStream dataFileStream,
        FileStream mergeFileStream,
        FileStream hintFileStream
    )
    {
        foreach (var nextEntry in FileOps.EnumerateRecordValues(dataFileStream))
            TryMergeSingleValue(nextEntry, mergeFileStream, hintFileStream);
    }

    private void TryMergeSingleValue(
        (string key, RecordValue value) fileEntry,
        FileStream mergeFileStream,
        FileStream hintFileStream
    )
    {
        // fail fast
        if (!ShouldMerge(fileEntry))
            return;

        // If the key is updated at this point, it won't cause incorrect behavior since
        // new value will be in a write log with file index greater than the merge file index

        WriteResult writeResult;
        // write in merge file first so that hint file never contains dangling pointers
        lock (FileMergeLock)
        { // lock so that read opeartions don't clobber this file handle
            mergeFileStream.Seek(0, SeekOrigin.End);
            writeResult = FileOps.WriteRecord(
                mergeFileStream,
                fileEntry.key,
                fileEntry.value.Value
            );
        }

        // should process fail at this point, the next keyDir initialization will point to the original data file
        // of this value since the data file would not be deleted. The duplicate entry in mergeFile will get
        // automatically ignored since hintFile won't have and entry.
        FileOps.WriteHintRecord(
            hintFileStream,
            fileEntry.key,
            writeResult.FileValue.ValueSize,
            writeResult.FileValue.ValuePosition
        );

        // updating keyDir should be done with a lock to ensure we don't lose new writes
        lock (LockManager.GetLockObject(fileEntry.key))
        {
            // check if the merge conditions still meet before updating the keyDir
            if (ShouldMerge(fileEntry))
                KeyDir.Insert(fileEntry.key, writeResult.FileValue);
        }
    }

    private bool ShouldMerge((string key, RecordValue value) fileEntry)
    {
        // if KeyDir does not have this key, skip
        if (!KeyDir.TryGetValue(fileEntry.key, out var keyDirEntry))
            return false;

        int fileEntryIndex = GetFileIndex(fileEntry.value.FileId ?? throw new ArgumentException());
        int keyDirEntryIndex = GetFileIndex(keyDirEntry.FileId ?? throw new ArgumentException());

        if (keyDirEntryIndex < fileEntryIndex)
        {
            // should not happen
            logger.Error(
                "FileId in keyDir older than the one in file. Key: {@key}, fileValue: {@fileVal}, keyDirValue: {@keyDirVal}",
                fileEntry.key,
                fileEntry,
                keyDirEntry
            );

            Environment.FailFast(
                $"FileId in keyDir older than the one in file. Key {fileEntry.key}, fileValue: {fileEntry}, keyDirValue: {keyDirEntry}"
            );
        }

        // if KeyDir value points to a newer file, skip
        if (keyDirEntryIndex > fileEntryIndex)
            return false;

        // same file, if newer entry exists in same file, skip
        if (keyDirEntry.ValuePosition > fileEntry.value.ValuePosition)
            return false;

        if (keyDirEntry.ValuePosition < fileEntry.value.ValuePosition)
        {
            // should not happen
            logger.Error(
                "Position in keyDir older than the one in file. Key: {@key}, fileValue: {@fileVal}, keyDirValue: {@keyDirVal}",
                fileEntry.key,
                fileEntry,
                keyDirEntry
            );

            Environment.FailFast(
                $"Position in keyDir older than the one in file. Key: {fileEntry.key}, fileValue: {fileEntry}, keyDirValue: {keyDirEntry}"
            );
        }

        return true;
    }

    public void Sync()
    {
        ActiveFileStream.Flush();
    }

    public void SyncAndClose()
    {
        Sync();
        ActiveFileStream.Close();
    }

    public void Dispose()
    {
        try
        {
            // Close Active File Stream
            SyncAndClose();

            // Close all read file handles
            foreach (var fileHandle in ReadFileHandles.Values)
            {
                fileHandle.Flush();
                fileHandle.Close();
            }
        }
        catch (ObjectDisposedException)
        {
            // ignore
        }
        finally
        {
            ReadFileHandles = new();
            GC.SuppressFinalize(this);
        }
    }
}
