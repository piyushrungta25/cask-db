using System.Collections.Concurrent;

namespace cask_db;

class LockManager
{
    private ConcurrentDictionary<string, object> lockDictionary = new();

    public object GetLockObject(string key)
    {
        // Keys are never removed from the lockDictionary
        // Expected behavior is that the same object is returned for every key
        // If the key already exists in the dictionary, get or add will return the existing object
        return lockDictionary.GetOrAdd(key, () => new object());
    }
}
