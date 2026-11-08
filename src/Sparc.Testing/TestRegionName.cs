namespace Sparc.Testing;

internal static class TestRegionName
{
    internal static string Create() => "sparc-test-" + Guid.NewGuid().ToString("N");
}
