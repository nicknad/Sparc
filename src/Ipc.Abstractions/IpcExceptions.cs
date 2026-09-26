namespace Ipc;

/// <summary>Base type for IPC transport errors raised by an <see cref="IIpcMemoryRegionFactory"/>.</summary>
public class IpcException : Exception
{
    public IpcException(string message) : base(message) { }

    public IpcException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>The selected transport is not available on the current operating system.</summary>
public sealed class IpcPlatformNotSupportedException : IpcException
{
    public IpcPlatformNotSupportedException(string message) : base(message) { }
}

/// <summary>A region did not appear (or did not become usable) within the configured timeout.</summary>
public sealed class IpcTimeoutException : IpcException
{
    public IpcTimeoutException(string message) : base(message) { }
}
