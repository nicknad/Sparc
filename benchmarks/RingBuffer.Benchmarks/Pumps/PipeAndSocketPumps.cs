using System.Buffers.Binary;
using System.IO.Pipes;
using System.Net;
using System.Net.Sockets;

namespace RingBuffer.Benchmarks.Pumps;

public sealed class NamedPipePump : TwoThreadPump
{
    private readonly NamedPipeServerStream _writer;
    private readonly NamedPipeClientStream _reader;
    private readonly byte[] _lengthBuffer = new byte[sizeof(int)];

    public NamedPipePump()
    {
        string name = "spsc-bench-" + Guid.NewGuid().ToString("N");
        _writer = new NamedPipeServerStream(name, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.None);
        _reader = new NamedPipeClientStream(".", name, PipeDirection.In, PipeOptions.None);

        Task connect = Task.Run(_writer.WaitForConnection);
        _reader.Connect();
        connect.GetAwaiter().GetResult();
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_lengthBuffer, payload.Length);
        _writer.Write(_lengthBuffer);
        _writer.Write(payload);
        return true;
    }

    public override bool TryConsume(Span<byte> destination)
    {
        _reader.ReadExactly(_lengthBuffer);
        int length = BinaryPrimitives.ReadInt32LittleEndian(_lengthBuffer);
        _reader.ReadExactly(destination[..length]);
        return true;
    }

    public override void Dispose()
    {
        base.Dispose();
        _writer.Dispose();
        _reader.Dispose();
    }
}

public sealed class TcpLoopbackPump : TwoThreadPump
{
    private readonly TcpListener _listener;
    private readonly TcpClient _client;
    private readonly NetworkStream _writer;
    private readonly NetworkStream _reader;
    private readonly byte[] _lengthBuffer = new byte[sizeof(int)];

    public TcpLoopbackPump()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        IPEndPoint endpoint = (IPEndPoint)_listener.LocalEndpoint;

        _client = new TcpClient { NoDelay = true };
        Task<TcpClient> accept = Task.Run(_listener.AcceptTcpClient);
        _client.Connect(endpoint);
        TcpClient server = accept.GetAwaiter().GetResult();
        server.NoDelay = true;

        _writer = server.GetStream();
        _reader = _client.GetStream();
    }

    public override bool TryPublish(ReadOnlySpan<byte> payload)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_lengthBuffer, payload.Length);
        _writer.Write(_lengthBuffer);
        _writer.Write(payload);
        return true;
    }

    public override bool TryConsume(Span<byte> destination)
    {
        _reader.ReadExactly(_lengthBuffer);
        int length = BinaryPrimitives.ReadInt32LittleEndian(_lengthBuffer);
        _reader.ReadExactly(destination[..length]);
        return true;
    }

    public override void Dispose()
    {
        base.Dispose();
        _writer.Dispose();
        _reader.Dispose();
        _client.Dispose();
        _listener.Stop();
    }
}
