using System.Collections.Concurrent;
using Serilog;

namespace cask_db;

class KeyDir : ConcurrentDictionary<string, FileValue>
{
    ILogger logger = Log.ForContext<KeyDir>();
    private FileOps _fileOps = new FileOps();

    public void InitializeWithDataFile(FileStream dataFile)
    {
        logger.Debug("Initializing keyDir with dataFile {df}", dataFile.Name);

        foreach (var (key, val) in _fileOps.EnumerateFileValues(dataFile))
        {
            logger.Debug("Read key: {@key} value: {@value}", key, val);

            if (val.ValueSize == 0)
                this.TryRemove(key, out _);
            else
                this[key] = val;
        }
    }

    public void InitializeWithHintFile(FileStream hintFile, string dataFileName)
    {
        logger.Debug(
            "Initializing keyDir with hintFile :{hf}, dataFileName: {dfn}",
            hintFile.Name,
            dataFileName
        );

        foreach (var (key, val) in _fileOps.EnumerateHintFileRecords(hintFile))
        {
            logger.Debug("Read key: {@key} value: {@value}", key, val);
            val.FileId = dataFileName;
            this[key] = val;
        }
    }

    internal void Insert(string key, FileValue val)
    {
        this[key] = val;
    }

    internal void Remove(string key)
    {
        this.TryRemove(key, out _);
    }
}
