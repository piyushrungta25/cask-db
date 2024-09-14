using System.Buffers.Binary;
using System.Text;
using Serilog;

namespace cask_db;

class FileOps
{
    ILogger logger = Log.ForContext<FileOps>();

    public WriteResult WriteRecord(FileStream fs, string key, string? val)
    {
        long initialPosition = fs.Position;

        byte[] keyBuffer = Encoding.UTF8.GetBytes(key);
        byte[] valueBuffer = new byte[0];

        if (val != null)
            valueBuffer = Encoding.UTF8.GetBytes(val);

        int keySize = keyBuffer.Length;
        int valueSize = valueBuffer.Length;

        Span<byte> lengthBuffer = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, keySize);
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer.Slice(4, 4), valueSize);

        fs.Write(lengthBuffer);
        fs.Write(keyBuffer);
        fs.Write(valueBuffer);

        return new WriteResult
        {
            BytesWritten = lengthBuffer.Length + keyBuffer.Length + valueBuffer.Length,
            FileValue = new FileValue
            {
                FileId = fs.Name,
                ValueSize = valueSize,
                ValuePosition = initialPosition + 8 + keySize
            }
        };
    }

    byte[] buffer = new byte[400 * 1024]; // 400 Kb max

    public WriteResult WriteRecordSingleBuffer(FileStream fs, string key, string? val)
    {
        long initialPosition = fs.Position;

        Span<byte> buf = buffer;

        int keyLen = Encoding.UTF8.GetBytes(key, buf.Slice(8)); // will throw argument exception if key is too long to fit in 128 bytes
        int valueLen = 0;

        if (val != null)
            valueLen = Encoding.UTF8.GetBytes(val, buf.Slice(8 + keyLen));

        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(0, 4), keyLen);
        BinaryPrimitives.WriteInt32BigEndian(buf.Slice(4, 4), valueLen);

        fs.Write(buf.Slice(0, 8 + keyLen + valueLen));
        // fs.Flush();

        return new WriteResult
        {
            BytesWritten = 8 + keyLen + valueLen,
            FileValue = new FileValue
            {
                FileId = fs.Name,
                ValueSize = valueLen,
                ValuePosition = initialPosition + 8 + keyLen
            }
        };
    }

    public void WriteHintRecord(FileStream fs, string key, int valueSize, long valuePosition)
    {
        byte[] keyBuffer = Encoding.UTF8.GetBytes(key);

        int keySize = keyBuffer.Length;

        Span<byte> lengthBuffer = new byte[16];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, keySize);
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer.Slice(4, 4), valueSize);
        BinaryPrimitives.WriteInt64BigEndian(lengthBuffer.Slice(8, 8), valuePosition);

        fs.Write(lengthBuffer);
        fs.Write(keyBuffer);
    }

    public string? ReadValueAtPosition(FileStream fs, int valueLen, long position)
    {
        if (valueLen == 0)
            return null;

        if (fs.Position != position)
            fs.Seek(position, SeekOrigin.Begin);

        byte[] valueBuffer = new byte[valueLen];
        if (fs.Read(valueBuffer) < valueLen)
            return null;

        return Encoding.UTF8.GetString(valueBuffer);
    }

    public (string key, FileValue fileValue)? ReadFileValue(FileStream fs)
    {
        return ReadFileValueAtPosition(fs, fs.Position);
    }

    private (string, FileValue)? ReadFileValueAtPosition(FileStream fs, long position)
    {
        if (fs.Position != position)
            fs.Seek(position, SeekOrigin.Begin);

        var lengths = ReadLengths(fs);
        if (lengths == null)
            return null;

        var (keyLen, valueLen) = lengths.Value;

        // read key
        byte[] keyBuffer = new byte[keyLen];
        if (fs.Read(keyBuffer) < keyLen)
            return null;

        string key = Encoding.UTF8.GetString(keyBuffer);

        var fileValue = new FileValue
        {
            ValuePosition = fs.Position,
            ValueSize = valueLen,
            FileId = fs.Name
        };

        return (key, fileValue);
    }

    public IEnumerable<(string key, FileValue fileValue)> EnumerateFileValues(FileStream fs)
    {
        var entry = ReadFileValue(fs);

        while (entry != null)
        {
            yield return entry.Value;
            entry = ReadFileValueAtPosition(fs, fs.Position + entry.Value.fileValue.ValueSize);
        }
    }

    public IEnumerable<(string key, FileValue fileValue)> EnumerateHintFileRecords(FileStream fs)
    {
        var entry = ReadHintRecord(fs);

        while (entry != null)
        {
            yield return entry.Value;
            entry = ReadHintRecord(fs);
        }
    }

    public (string key, FileValue fileValue)? ReadHintRecord(FileStream fs)
    {
        Span<byte> lengths = new byte[16]; // 2 lengths + 1 offset

        if (fs.Read(lengths) < 16)
            return null;

        int keyLen = BinaryPrimitives.ReadInt32BigEndian(lengths.Slice(0, 4));
        int valueLen = BinaryPrimitives.ReadInt32BigEndian(lengths.Slice(4, 4));
        long valuePosition = BinaryPrimitives.ReadInt64BigEndian(lengths.Slice(8, 8));

        byte[] keyBuf = new byte[keyLen];

        if (fs.Read(keyBuf) < keyLen)
            return null;

        string key = Encoding.UTF8.GetString(keyBuf);

        return (key, new FileValue { ValuePosition = valuePosition, ValueSize = valueLen, });
    }

    public (string key, RecordValue recordValue)? ReadRecordValue(FileStream fs)
    {
        var lengths = ReadLengths(fs);
        if (lengths == null)
            return null;

        var (keyLen, valueLen) = lengths.Value;

        // read key and value
        Span<byte> buffer = new byte[keyLen + valueLen];
        if (fs.Read(buffer) < (keyLen + valueLen))
            return null;

        string key = Encoding.UTF8.GetString(buffer.Slice(0, keyLen));
        string? value =
            valueLen == 0 ? null : Encoding.UTF8.GetString(buffer.Slice(keyLen, valueLen));

        var recordValue = new RecordValue
        {
            ValuePosition = fs.Position - valueLen,
            ValueSize = valueLen,
            FileId = fs.Name,
            Value = value
        };

        return (key, recordValue);
    }

    public IEnumerable<(string key, RecordValue fileValue)> EnumerateRecordValues(FileStream fs)
    {
        var entry = ReadRecordValue(fs);

        while (entry != null)
        {
            yield return entry.Value;
            entry = ReadRecordValue(fs);
        }
    }

    private (int, int)? ReadLengths(FileStream fs)
    {
        Span<byte> lengths = new byte[8]; // 2 lengths

        if (fs.Read(lengths) < 8)
            return null;

        int keyLen = BinaryPrimitives.ReadInt32BigEndian(lengths.Slice(0, 4));
        int valueLen = BinaryPrimitives.ReadInt32BigEndian(lengths.Slice(4, 4));

        return (keyLen, valueLen);
    }

    #region debug

    // for debugging only
    public Record? ReadRecord(FileStream fs)
    {
        return ReadRecordAtPosition(fs, fs.Position);
    }

    // for debugging only
    public Record? ReadRecordAtPosition(FileStream fs, long position)
    {
        if (fs.Position != position)
            fs.Seek(position, SeekOrigin.Begin);

        var lengths = ReadLengths(fs);
        if (lengths == null)
            return null;

        var (keyLen, valueLen) = lengths.Value;

        int keyValLength = keyLen + valueLen;
        byte[] keyValBuffer = new byte[keyValLength];

        if (fs.Read(keyValBuffer) != keyValLength)
            return null;

        return new Record
        {
            Key = Encoding.UTF8.GetString(keyValBuffer, 0, keyLen),
            Value = valueLen == 0 ? null : Encoding.UTF8.GetString(keyValBuffer, keyLen, valueLen)
        };
    }

    // For debugging only
    public IEnumerable<Record> EnumerateRecords(FileStream fs)
    {
        var record = ReadRecord(fs);
        while (record != null)
        {
            yield return record;
            record = ReadRecord(fs);
        }
    }

    # endregion
}
