namespace Sparc.Client;

/// <summary>
/// The pair of notification latches a producer and consumer share:
/// <see cref="Data"/> (raised by the producer after publishing, waited on by the
/// consumer when empty) and <see cref="Space"/> (raised by the consumer after
/// consuming, waited on by the producer when full).
/// </summary>
/// <remarks>
/// Both endpoints of a session pair must be configured with the same pair, and
/// the host owns the lifetime (the sessions never dispose it).
/// </remarks>
public sealed class SessionNotification : IDisposable
{
    public SessionNotification(ISessionSignal data, ISessionSignal space)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(space);
        Data = data;
        Space = space;
    }

    /// <summary>Producer raises it after publishing; consumer waits on it when empty.</summary>
    public ISessionSignal Data { get; }

    /// <summary>Consumer raises it after consuming; producer waits on it when full.</summary>
    public ISessionSignal Space { get; }

    /// <summary>Creates a same-process pair for tests, samples and single-process hosts.</summary>
    public static SessionNotification CreateInProcess() =>
        new(new InProcessSessionSignal(), new InProcessSessionSignal());

    /// <summary>
    /// Creates a cross-process pair of named OS latches derived from the region
    /// name. Windows only; throws <see cref="PlatformNotSupportedException"/>
    /// elsewhere.
    /// </summary>
    /// <param name="regionName">Name of the ring region both endpoints share.</param>
    public static SessionNotification CreateNamed(string regionName)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionName);
        return new SessionNotification(
            new NamedSessionSignal(regionName + ".data"),
            new NamedSessionSignal(regionName + ".space"));
    }

    public void Dispose()
    {
        Data.Dispose();
        Space.Dispose();
    }
}
