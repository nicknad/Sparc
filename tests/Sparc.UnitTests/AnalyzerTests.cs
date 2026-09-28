using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Sparc.Analyzers;
using Sparc.Client;

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

    private static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync(string source)
    {
        List<MetadataReference> references = [];
        foreach (string path in ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator))
        {
            references.Add(MetadataReference.CreateFromFile(path));
        }

        references.Add(MetadataReference.CreateFromFile(typeof(ProducerSessionOptions).Assembly.Location));

        CSharpCompilation compilation = CSharpCompilation.Create(
            "AnalyzerTest",
            [CSharpSyntaxTree.ParseText(source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        return await compilation
            .WithAnalyzers([new NotificationModeAnalyzer()])
            .GetAnalyzerDiagnosticsAsync()
            .ConfigureAwait(false);
    }
}
