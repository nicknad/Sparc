using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Sparc.Analyzers;

/// <summary>
/// SPARC0003: endpoints are single-owner and thread-affine — one thread drives
/// the producer, one the consumer, for the lifetime of the buffer. A
/// <c>static</c> field invites sharing across threads or tests, which races on
/// the pending/lease state and the owned cursor. Prefer a local, an instance
/// field, or DI scoping.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SharedEndpointAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "SPARC0003";

    private static readonly DiagnosticDescriptor Rule = new(
        id: DiagnosticId,
        title: "Endpoints must not be shared through static fields",
        messageFormat: "Do not store {0} in a static field; endpoints are single-owner and thread-affine",
        category: "Usage",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "SPSC endpoints keep thread-affine pending/lease state and an owned cursor. " +
            "A static field invites concurrent or cross-test use; keep the endpoint in a local, " +
            "an instance field, or a scoped service.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Rule);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeField, SymbolKind.Field);
    }

    private static void AnalyzeField(SymbolAnalysisContext context)
    {
        var field = (IFieldSymbol)context.Symbol;
        if (!field.IsStatic)
        {
            return;
        }

        // OriginalDefinition strips any nullable annotation (IProducerEndpoint? would
        // otherwise display with a trailing '?').
        string? typeName = field.Type?.OriginalDefinition.ToDisplayString();
        if (string.Equals(typeName, "Sparc.Core.IProducerEndpoint", StringComparison.Ordinal)
            || string.Equals(typeName, "Sparc.Core.IConsumerEndpoint", StringComparison.Ordinal)
            || string.Equals(typeName, "Sparc.Core.SpscRingBuffer", StringComparison.Ordinal))
        {
            foreach (SyntaxReference reference in field.DeclaringSyntaxReferences)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rule, reference.GetSyntax().GetLocation(), typeName));
            }
        }
    }
}
