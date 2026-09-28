using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Sparc.Analyzers;

/// <summary>
/// SPARC0001: a session options object that selects
/// <c>SessionWaitMode.Notification</c> must also set a
/// <c>SessionNotification</c>; without one the session constructor throws.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NotificationModeAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "SPARC0001";

    private const string ProducerOptionsType = "Sparc.Client.ProducerSessionOptions";
    private const string ConsumerOptionsType = "Sparc.Client.ConsumerSessionOptions";
    private const string WaitModeType = "Sparc.Client.SessionWaitMode";

    private static readonly DiagnosticDescriptor Rule = new(
        id: DiagnosticId,
        title: "Notification wait mode requires a SessionNotification",
        messageFormat: "Set the Notification property when WaitMode is SessionWaitMode.Notification",
        category: "Usage",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Sparc.Client.ProducerSessionOptions and ConsumerSessionOptions require a Notification " +
            "instance when WaitMode is SessionWaitMode.Notification.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Rule);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeObjectCreation, OperationKind.ObjectCreation);
    }

    private static void AnalyzeObjectCreation(OperationAnalysisContext context)
    {
        IObjectCreationOperation creation = (IObjectCreationOperation)context.Operation;
        string? typeName = creation.Type?.ToDisplayString();
        if (!string.Equals(typeName, ProducerOptionsType, StringComparison.Ordinal)
            && !string.Equals(typeName, ConsumerOptionsType, StringComparison.Ordinal))
        {
            return;
        }

        bool waitModeIsNotification = false;
        bool notificationSet = false;

        if (creation.Initializer is { } initializer)
        {
            foreach (IOperation operation in initializer.Initializers)
            {
                if (operation is not ISimpleAssignmentOperation assignment
                    || assignment.Target is not IPropertyReferenceOperation property)
                {
                    continue;
                }

                string propertyName = property.Property.Name;
                if (string.Equals(propertyName, "Notification", StringComparison.Ordinal))
                {
                    notificationSet = !IsNullLiteral(assignment.Value);
                }
                else if (string.Equals(propertyName, "WaitMode", StringComparison.Ordinal)
                    && IsNotificationEnum(assignment.Value))
                {
                    waitModeIsNotification = true;
                }
            }
        }

        if (waitModeIsNotification && !notificationSet)
        {
            context.ReportDiagnostic(Diagnostic.Create(Rule, creation.Syntax.GetLocation()));
        }
    }

    private static bool IsNullLiteral(IOperation operation)
    {
        if (operation is IConversionOperation { IsImplicit: true } conversion)
        {
            operation = conversion.Operand;
        }

        return operation is ILiteralOperation { ConstantValue: { HasValue: true, Value: null } };
    }

    private static bool IsNotificationEnum(IOperation operation) =>
        operation is IFieldReferenceOperation field
        && string.Equals(field.Field.Name, "Notification", StringComparison.Ordinal)
        && string.Equals(field.Field.ContainingType?.ToDisplayString(), WaitModeType, StringComparison.Ordinal);
}
