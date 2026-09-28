using CsCheck;

namespace Sparc.FuzzTests;

public class RegionNameFuzzTests
{
    [Fact]
    public void ArbitraryNamesEitherThrowOrHaveNoPathSemantics()
    {
        Gen.Char.Array.Select(chars => new string(chars)).Sample(
            name =>
            {
                try
                {
                    RegionName.Validate(name);
                }
                catch (ArgumentException)
                {
                    return;
                }

                Assert.DoesNotContain('\\', name);
                Assert.DoesNotContain('/', name);
                Assert.DoesNotContain('\0', name);
                Assert.False(name is "." or "..");
            },
            iter: 10_000);
    }
}
