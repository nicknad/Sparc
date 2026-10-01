using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Sparc;
using Sparc.Analyzers;
using Sparc.Client;
using Sparc.Core;

namespace Sparc.UnitTests;

public class AnalyzerTests
{
    [Fact]
    public async Task FlagsNotificationWaitModeWithoutNotification()
    {
        const string Source = """
            using Sparc.Client;

            class C
            {
                ProducerSessionOptions Create() => new ProducerSessionOptions
                {
                    WaitMode = SessionWaitMode.Notification,
                };
            }
            """;

        ImmutableArray<Diagnostic> diagnostics = await GetDiagnosticsAsync(Source);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(NotificationModeAnalyzer.DiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task AcceptsNotificationWaitModeWithNotification()
    {
        const string Source = """
            using Sparc.Client;

            class C
            {
                ConsumerSessionOptions Create(SessionNotification notification) => new ConsumerSessionOptions
                {
                    WaitMode = SessionWaitMode.Notification,
                    Notification = notification,
                };
            }
            """;

        Assert.Empty(await GetDiagnosticsAsync(Source));
    }

    [Fact]
    public async Task AcceptsOtherWaitModesWithoutNotification()
    {
        const string Source = """
            using Sparc.Client;

            class C
            {
                ProducerSessionOptions Create() => new ProducerSessionOptions
                {
                    WaitMode = SessionWaitMode.SpinOnly,
                };
            }
            """;

        Assert.Empty(await GetDiagnosticsAsync(Source));
    }

    [Fact]
    public async Task FlagsExplicitNullNotification()
    {
        const string Source = """
            using Sparc.Client;

            class C
            {
                ConsumerSessionOptions Create() => new ConsumerSessionOptions
                {
                    WaitMode = SessionWaitMode.Notification,
                    Notification = null,
                };
            }
            """;

        ImmutableArray<Diagnostic> diagnostics = await GetDiagnosticsAsync(Source);

        Assert.Single(diagnostics);
    }

    [Fact]
    public async Task FlagsNonPowerOfTwoCapacity()
    {
        const string Source = """
            using Sparc;
            using Sparc.Core;

            class C
            {
                void M(IIpcMemoryRegionFactory factory)
                {
                    SparcRing.OpenProducer(factory, "orders", 1000, 256);
                }
            }
            """;

        ImmutableArray<Diagnostic> diagnostics = await GetDiagnosticsAsync(Source, new GeometryAnalyzer());

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(GeometryAnalyzer.DiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task FlagsUndersizedSlot()
    {
        const string Source = """
            using Sparc.Core;

            class C
            {
                SpscRingBuffer Create() => new SpscRingBuffer(1024, 8);
            }
            """;

        ImmutableArray<Diagnostic> diagnostics = await GetDiagnosticsAsync(Source, new GeometryAnalyzer());

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(GeometryAnalyzer.DiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task AcceptsValidGeometry()
    {
        const string Source = """
            using Sparc;
            using Sparc.Core;

            class C
            {
                void M(IIpcMemoryRegionFactory factory)
                {
                    SparcRing.OpenProducer(factory, "orders", 1024, 256);
                }
            }
            """;

        Assert.Empty(await GetDiagnosticsAsync(Source, new GeometryAnalyzer()));
    }

    [Fact]
    public async Task FlagsStaticEndpointField()
    {
        const string Source = """
            using Sparc.Core;

            class C
            {
                private static IProducerEndpoint? _producer;
            }
            """;

        ImmutableArray<Diagnostic> diagnostics = await GetDiagnosticsAsync(Source, new SharedEndpointAnalyzer());

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(SharedEndpointAnalyzer.DiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task AcceptsInstanceEndpointField()
    {
        const string Source = """
            using Sparc.Core;

            class C
            {
                private IProducerEndpoint? _producer;
            }
            """;

        Assert.Empty(await GetDiagnosticsAsync(Source, new SharedEndpointAnalyzer()));
    }

    private static Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source) =>
        GetDiagnosticsAsync(source, new NotificationModeAnalyzer());

    private static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(
        string source, DiagnosticAnalyzer analyzer)
    {
        List<MetadataReference> references = [];
        foreach (string path in ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator))
        {
            references.Add(MetadataReference.CreateFromFile(path));
        }

        references.Add(MetadataReference.CreateFromFile(typeof(ProducerSessionOptions).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(SpscRingBuffer).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(IIpcMemoryRegionFactory).Assembly.Location));

        CSharpCompilation compilation = CSharpCompilation.Create(
            "AnalyzerTest",
            [CSharpSyntaxTree.ParseText(source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        return await compilation
            .WithAnalyzers([analyzer])
            .GetAnalyzerDiagnosticsAsync()
            .ConfigureAwait(false);
    }
}
