using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

public class Producer
{
    private readonly ChannelWriter<int> _writer;

    public Producer(ChannelWriter<int> writer)
    {
        _writer = writer;
    }

    public async Task ProduceAsync()
    {
        for (int i = 0; i < 5000; i++)
        {
            await _writer.WriteAsync(i);
        }

        _writer.Complete();
    }
}

public class Consumer
{
    private readonly ChannelReader<int> _reader;

    public Consumer(ChannelReader<int> reader)
    {
        _reader = reader;
    }

    public async Task ConsumeAsync()
    {
        while (await _reader.WaitToReadAsync())
        {
            if (_reader.TryRead(out var item))
            {
                Console.WriteLine($"Processed {item} on thread {Task.CurrentId}");
            }
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        var channel = Channel.CreateUnbounded<int>();

        var producer = new Producer(channel.Writer);
        var consumers = Enumerable.Range(0, 10).Select(_ => new Consumer(channel.Reader)).ToList();

        var producerTask = producer.ProduceAsync();
        var consumerTasks = consumers.Select(c => c.ConsumeAsync()).ToArray();

        await Task.WhenAll(consumerTasks.Concat(new[] { producerTask }));
    }
}
