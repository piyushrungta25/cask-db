namespace cask_db;

// For debugging only
class Record
{
    public string? Key { get; set; }
    public string? Value { get; set; }
}

class FileValue
{
    public string? FileId { get; set; }
    public int ValueSize { get; set; }
    public long ValuePosition { get; set; }
}

class RecordValue : FileValue
{
    public string? Value { get; set; }

    public FileValue ToFileValue()
    {
        return new FileValue
        {
            FileId = this.FileId,
            ValueSize = this.ValueSize,
            ValuePosition = this.ValuePosition
        };
    }
}

class WriteResult
{
    public required int BytesWritten { get; init; }
    public required FileValue FileValue { get; init; }
}
