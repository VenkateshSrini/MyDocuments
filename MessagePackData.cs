using MessagePack.Formatters;
using MessagePack.Resolvers;
using MessagePack;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System;

public class MessagePackData
{
    //private readonly byte[] _data;
    private ReadOnlyMemory<byte> _data;
    public int Length => _data.Length;
    public bool IsEmpty => _data.IsEmpty;
    public static implicit operator ReadOnlyMemory<byte>(MessagePackData? data) => data?._data ?? default;
    public static implicit operator ReadOnlySpan<byte>(MessagePackData? data)
    {
        if (data == null)
        {
            return default;
        }
        return data._data.Span;
        
    }
 


    private MessagePackData(byte[] data)
    {
        _data = data;
    }
    private MessagePackData(ReadOnlyMemory<byte> data)
    {
        _data = data;
    }
    public static MessagePackData FromObject<T>(T obj, MessagePackSerializerOptions Paramoptions)
    {
        MessagePackSerializerOptions options;


        if (Paramoptions == null)
        {
            options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
        }
        else
            options = Paramoptions;


        using var stream = new MemoryStream();
        var pipeWriter = PipeWriter.Create(stream);
        var writer = new MessagePackWriter(pipeWriter);
        options.Resolver.GetFormatterWithVerify<T>().Serialize(ref writer, obj, options);
        writer.Flush();
        pipeWriter.FlushAsync().GetAwaiter().GetResult();

        return new MessagePackData(stream.ToArray());
    }
    public static MessagePackData FromBytes(ReadOnlyMemory<byte> data) => new(data);
    public static Task<MessagePackData> FromStreamAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        if (stream is null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        return FromStreamAsync(stream, async: true, cancellationToken: cancellationToken);
    }
    public static MessagePackData FromStream(Stream stream, string? mediaType)
    {
        if (stream is null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        return FromStreamAsync(stream, async: false).GetAwaiter().GetResult();
    }
    private static async Task<MessagePackData> FromStreamAsync(Stream stream, bool async,
            CancellationToken cancellationToken = default)
    {
        const int CopyToBufferSize = 81920;  // the default used by Stream.CopyToAsync
        int bufferSize = CopyToBufferSize;
        MemoryStream memoryStream;

        if (stream.CanSeek)
        {
            long longLength = stream.Length - stream.Position;
            if (longLength > int.MaxValue || longLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(stream),"Stream must not be empty");
            }

            // choose a minimum valid (non-zero) buffer size.
            bufferSize = longLength == 0 ? 1 : Math.Min((int)longLength, CopyToBufferSize);
            memoryStream = new MemoryStream((int)longLength);
        }
        else
        {
            memoryStream = new MemoryStream();
        }

        using (memoryStream)
        {
            if (async)
            {
                await stream.CopyToAsync(memoryStream, bufferSize, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                stream.CopyTo(memoryStream, bufferSize);
            }
            return new MessagePackData(memoryStream.GetBuffer().AsMemory(0, (int)memoryStream.Position));
        }
    }

    public T ToObject<T>(MessagePackSerializerOptions Paramoptions)
    {
        MessagePackSerializerOptions options;

        if (Paramoptions == null)
        {
            options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
        }
        else
            options = Paramoptions;

        var sequence = new ReadOnlySequence<byte>(_data);
        var reader = new MessagePackReader(sequence);
        return options.Resolver.GetFormatterWithVerify<T>().Deserialize(ref reader, options);
    }
    public ReadOnlyMemory<byte> ToMemory() => _data;
    public byte[] ToArray() => _data.ToArray();
    public Stream ToStream() => new MemoryStream(_data.ToArray());
}
