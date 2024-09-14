namespace cask_db;

public class CaskDbOpts
{
    public required string DatabaseDirectory { get; set; }
    public int DataFileSizeThresholdInBytes { get; init; } = 16 * 1024 * 1024;
    public bool SoftDeleteDataFiles { get; init; } = true;
    public bool UseSynchronousWrites { get; init; } = false;
}
