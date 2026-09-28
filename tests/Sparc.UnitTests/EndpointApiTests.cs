using Sparc;
using Sparc.Core;
using Sparc.InMemory;

namespace Sparc.UnitTests;

public class EndpointApiTests
{
    private static string NewName() => "spsc-endpoint-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void OpenProducerAndConsumerRoundTripMessages()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RingBufferEndpointState.Running, producer.ProducerState);
        Assert.Equal(RingBufferEndpointState.Running, consumer.ConsumerState);
        Assert.True(producer.TryPublish(7, new byte[] { 1, 2, 3 }));

        Span<byte> destination = new byte[consumer.MaxPayloadSize];
        Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(3, bytesRead);
        Assert.Equal(7, type);
        Assert.Equal(new byte[] { 1, 2, 3 }, destination[..bytesRead].ToArray());
    }

    [Fact]
    public void RawReservationPublishesTheCommittedPrefix()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producer.TryReserveWrite(7, out Span<byte> payload));
        Assert.Equal(producer.MaxPayloadSize, payload.Length);
        payload[0] = 4;
        payload[1] = 5;
        payload[2] = 6;
        producer.CommitWrite(3);

        Span<byte> destination = new byte[consumer.MaxPayloadSize];
        Assert.True(consumer.TryRead(destination, out int bytesRead, out int type));
        Assert.Equal(3, bytesRead);
        Assert.Equal(7, type);
        Assert.Equal(new byte[] { 4, 5, 6 }, destination[..bytesRead].ToArray());
    }

    [Fact]
    public void CommitLengthBeyondTheReservationThrowsAndKeepsItActive()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producer.TryReserveWrite(1, 2, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => producer.CommitWrite(3));

        producer.CommitWrite();
        Span<byte> destination = new byte[consumer.MaxPayloadSize];
        Assert.True(consumer.TryRead(destination, out int bytesRead, out _));
        Assert.Equal(2, bytesRead);
    }

    [Fact]
    public void WriteLeaseCommitsOnDispose()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        using (WriteLease lease = producer.BeginWrite(3, 4, TestContext.Current.CancellationToken))
        {
            lease.Payload.Fill(0x11);
        }

        using (ReadLease read = consumer.Read(TestContext.Current.CancellationToken))
        {
            Assert.Equal(3, read.Type);
            Assert.Equal(4, read.Length);
            Assert.True(read.Payload.IndexOfAnyExcept((byte)0x11) < 0);
        }

        Assert.Equal(1, consumer.HeadSequence);
    }

    [Fact]
    public void AbandonedWriteLeasePublishesNothing()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        using (WriteLease lease = producer.BeginWrite(0, 4, TestContext.Current.CancellationToken))
        {
            lease.Payload.Fill(0x22);
            lease.Abandon();
        }

        Assert.True(consumer.IsEmpty);
        Assert.True(producer.TryBeginWrite(0, 4, out WriteLease reused));
        reused.Dispose();
    }

    [Fact]
    public void ReadLeaseReleasesTheSlotOnDispose()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(producer.TryPublish(new byte[] { 9, 8, 7 }));
        Assert.True(consumer.TryBeginRead(out ReadLease lease));
        Assert.Equal(3, lease.Length);
        Assert.Equal(new byte[] { 9, 8, 7 }, lease.Payload.ToArray());
        Assert.Equal(0, consumer.HeadSequence);

        lease.Dispose();
        Assert.Equal(1, consumer.HeadSequence);
        Assert.True(consumer.IsEmpty);
    }

    [Fact]
    public void RoleConflictIsRejectedUnlessTakeoverRequested()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint first = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Throws<RingBufferRoleConflictException>(() => SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken));

        using IProducerEndpoint replacement = SparcRing.OpenProducer(
            factory, name, 4, 64,
            new SharedRingBufferOptions { Takeover = true },
            TestContext.Current.CancellationToken);
        Assert.Equal(RingBufferEndpointState.Running, replacement.ProducerState);
    }

    [Fact]
    public void OpenHonorsCancellationAndReleasesTheRegion()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () => SparcRing.OpenProducer(factory, name, 4, 64, cancellationToken: cancellation.Token));

        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(RingBufferEndpointState.Running, producer.ProducerState);
    }

    [Fact]
    public void BlockingReadHonorsCancellation()
    {
        InMemoryMemoryRegionFactory factory = new();
        string name = NewName();
        using IProducerEndpoint producer = SparcRing.OpenProducer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using IConsumerEndpoint consumer = SparcRing.OpenConsumer(
            factory, name, 4, 64, cancellationToken: TestContext.Current.CancellationToken);
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(
            () =>
            {
                ReadLease lease = consumer.Read(cancellation.Token);
                lease.Dispose();
            });
    }

    [Fact]
    public void ErrorCodesClassifyExceptions()
    {
        Assert.Equal(SparcErrorCode.Unknown, new SparcException("x").Code);
        Assert.Equal(SparcErrorCode.Timeout, new RingBufferTimeoutException("x").Code);
        Assert.Equal(SparcErrorCode.Timeout, new IpcTimeoutException("x").Code);
        Assert.Equal(SparcErrorCode.RoleConflict, new RingBufferRoleConflictException("x").Code);
        Assert.Equal(SparcErrorCode.Corrupted, new RingBufferCorruptedException("x").Code);
        Assert.Equal(SparcErrorCode.VersionMismatch, new RingBufferVersionMismatchException("x").Code);
        Assert.Equal(SparcErrorCode.GeometryMismatch, new RingBufferGeometryMismatchException("x").Code);
        Assert.Equal(SparcErrorCode.PlatformNotSupported, new IpcPlatformNotSupportedException("x").Code);
        Assert.Equal(SparcErrorCode.PlatformNotSupported, new RingBufferPlatformNotSupportedException("x").Code);
    }

    [Fact]
    public void RegionNameValidationRejectsPathLikeNames()
    {
        Assert.Equal("orders", RegionName.Validate("orders"));
        Assert.Throws<ArgumentException>(() => RegionName.Validate(""));
        Assert.Throws<ArgumentException>(() => RegionName.Validate("a/b"));
        Assert.Throws<ArgumentException>(() => RegionName.Validate("a\\b"));
        Assert.Throws<ArgumentException>(() => RegionName.Validate(".."));
    }
}
