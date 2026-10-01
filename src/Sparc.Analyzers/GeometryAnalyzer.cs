using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Sparc.Analyzers;

/// <summary>
/// SPARC0002: ring geometry passed as literals is validated at compile time.
/// A capacity that is not a power of two, or a slot size that cannot hold the
/// 8-byte message header, throws <c>ArgumentOutOfRangeException</c> at runtime
/// (<c>RingBufferLayout.ValidateGeometry</c>); flag it where it is written.
/// Only integer literals are checked — computed values still fail at runtime.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class GeometryAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "SPARC0002";

    private static readonly DiagnosticDescriptor Rule = new(
        id: DiagnosticId,
        title: "Invalid ring geometry",
        messageFormat: "{0}",
        category: "Usage",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Capacity must be a positive power of two and slot size must exceed " +
            "the 8-byte message header (RingBufferLayout.ValidateGeometry).");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Rule);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
        context.RegisterOperationAction(AnalyzeObjectCreation, OperationKind.ObjectCreation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        var invocation = (IInvocationOperation)context.Operation;
        string? typeName = invocation.TargetMethod.ContainingType?.ToDisplayString();
        if (!string.Equals(typeName, "Sparc.Core.SparcRing", StringComparison.Ordinal))
        {
            return;
        }

        string method = invocation.TargetMethod.Name;
        if (!string.Equals(method, "OpenProducer", StringComparison.Ordinal)
            && !string.Equals(method, "OpenConsumer", StringComparison.Ordinal))
        {
            return;
        }

        // Signatures: (factory, name, capacity = 1024, slotSize = 256, ...).
        CheckCapacity(context, ArgumentLiteral(invocation.Arguments, "capacity", 2));
        CheckSlotSize(context, ArgumentLiteral(invocation.Arguments, "slotSize", 3));
    }

    private static void AnalyzeObjectCreation(OperationAnalysisContext context)
    {
        var creation = (IObjectCreationOperation)context.Operation;
        if (!string.Equals(creation.Type?.ToDisplayString(), "Sparc.Core.SpscRingBuffer", StringComparison.Ordinal))
        {
            return;
        }

        // SpscRingBuffer(int capacity, int slotSize).
        CheckCapacity(context, ArgumentLiteral(creation.Arguments, "capacity", 0));
        CheckSlotSize(context, ArgumentLiteral(creation.Arguments, "slotSize", 1));
    }

    private static int? ArgumentLiteral(ImmutableArray<IArgumentOperation> arguments, string name, int position)
    {
        IArgumentOperation? byName = arguments.FirstOrDefault(a =>
            string.Equals(a.Parameter?.Name, name, StringComparison.Ordinal));
        IArgumentOperation? argument = byName ?? (position < arguments.Length ? arguments[position] : null);
        return LiteralInt(argument?.Value);
    }

    private static int? LiteralInt(IOperation? operation)
    {
        if (operation is IConversionOperation { IsImplicit: true } conversion)
        {
            operation = conversion.Operand;
        }

        return operation is ILiteralOperation { ConstantValue: { HasValue: true, Value: int value } }
            ? value
            : null;
    }

    private static void CheckCapacity(OperationAnalysisContext context, int? capacity)
    {
        if (capacity is not { } value)
        {
            return;
        }

        if (value <= 0 || (value & (value - 1)) != 0)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rule, context.Operation.Syntax.GetLocation(),
                $"Capacity {value} is not a positive power of two; slot indexing requires one."));
        }
    }

    private static void CheckSlotSize(OperationAnalysisContext context, int? slotSize)
    {
        if (slotSize is not { } value)
        {
            return;
        }

        if (value <= 8)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rule, context.Operation.Syntax.GetLocation(),
                $"Slot size {value} must exceed the 8-byte message header."));
        }
    }
}
