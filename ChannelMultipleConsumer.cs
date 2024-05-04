using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
namespace StringBuilderToMemStream;
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
                // Call your synchronous method here
                await Task.Run(() => ProcessItem(item));
            }
        }

        //await foreach (var item in _reader.ReadAllAsync())
        //{
        //    await Task.Run(() => ProcessItem(item));
        //}
    }

    private void ProcessItem(int item)
    {
        // Simulate processing time
        System.Threading.Thread.Sleep(100);
        Console.WriteLine($"Processed {item} on thread {Task.CurrentId}");
    }
}

class EntryPoint
{
    public void Run()
    {
        var channel = Channel.CreateUnbounded<int>();

        var producer = new Producer(channel.Writer);
        var consumers = Enumerable.Range(0, 10).Select(_ => new Consumer(channel.Reader)).ToList();

        var producerTask = producer.ProduceAsync();
        var consumerTasks = consumers.Select(c => c.ConsumeAsync()).ToArray();

        Task.WaitAll(consumerTasks.Concat(new[] { producerTask }).ToArray());
    }
    
}
