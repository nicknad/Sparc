namespace Sparc.Benchmarks;

/// <summary>
/// One measured scenario in the regression matrix: transfers
/// <paramref name="count"/> messages of <paramref name="payloadSize"/> bytes and
/// returns the elapsed seconds.
/// </summary>
/// <remarks>
/// <see cref="TwoThreadPump"/> is the per-message shape, where the scenario owns
/// the transfer and the base class owns the two threads. A scenario that drives
/// a whole loop itself (a producer/consumer session pair) implements this
/// interface directly and owns its threads.
/// </remarks>
public interface IScenarioPump : IDisposable
{
    /// <summary>Transfers one batch and returns how long it took.</summary>
    double Run(long count, int payloadSize);
}
