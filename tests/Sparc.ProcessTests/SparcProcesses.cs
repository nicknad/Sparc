using System.Diagnostics;

namespace Sparc.ProcessTests;

internal sealed record ProcessResult(int ExitCode, string StdOut, string StdErr);

internal static class SparcProcesses
{
    public static string RepoRoot { get; } = FindRepoRoot();

#if DEBUG
    public const string Configuration = "Debug";
#else
    public const string Configuration = "Release";
#endif

    public static string ProducerDll { get; } = Path.Combine(
        RepoRoot, "tools", "Sparc.Producer", "bin", Configuration, "net11.0", "Sparc.Producer.dll");

    public static string ConsumerDll { get; } = Path.Combine(
        RepoRoot, "tools", "Sparc.Consumer", "bin", Configuration, "net11.0", "Sparc.Consumer.dll");

    public static string NewName() => "spsc-proc-" + Guid.NewGuid().ToString("N");

    public static Process StartProducer(params string[] args) => Start(ProducerDll, args);

    public static Process StartConsumer(params string[] args) => Start(ConsumerDll, args);

    public static Process Start(string dll, params string[] args)
    {
        ProcessStartInfo startInfo = new("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = RepoRoot,
        };

        startInfo.ArgumentList.Add(dll);
        foreach (string arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        return Process.Start(startInfo)
            ?? throw new InvalidOperationException($"Failed to start 'dotnet {dll}'.");
    }

    /// <summary>Reads output lines until the endpoint prints its "ready:" banner.</summary>
    public static async Task<string> WaitForReadyAsync(Process process, int timeoutMs = 30_000)
    {
        using CancellationTokenSource cts = new(timeoutMs);

        while (true)
        {
            string? line = await process.StandardOutput.ReadLineAsync(cts.Token).ConfigureAwait(false);
            if (line is null)
            {
                throw new InvalidOperationException(
                    $"Process exited with code {process.ExitCode} before becoming ready.");
            }

            if (line.StartsWith("ready:", StringComparison.Ordinal))
            {
                return line;
            }
        }
    }

    public static async Task<ProcessResult> WaitAsync(Process process, int timeoutMs)
    {
        Task<string> stdout = process.StandardOutput.ReadToEndAsync();
        Task<string> stderr = process.StandardError.ReadToEndAsync();

        using CancellationTokenSource cts = new(timeoutMs);
        try
        {
            await process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Kill(process);
            await process.WaitForExitAsync().ConfigureAwait(false);
            return new ProcessResult(-1, await stdout.ConfigureAwait(false), await stderr.ConfigureAwait(false));
        }

        return new ProcessResult(process.ExitCode, await stdout.ConfigureAwait(false), await stderr.ConfigureAwait(false));
    }

    public static void Kill(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
        catch (InvalidOperationException)
        {
        }
        catch (System.ComponentModel.Win32Exception)
        {
        }
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Sparc.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate the repository root (Sparc.slnx).");
    }
}
