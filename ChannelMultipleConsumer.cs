using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Newtonsoft.Json;

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
    private readonly ConcurrentBag<int> _bag;

    public Consumer(ChannelReader<int> reader, ConcurrentBag<int> bag)
    {
        _reader = reader;
        _bag = bag;
    }

    public async Task ConsumeAsync()
    {
        await foreach (var item in _reader.ReadAllAsync())
        {
            if (await ProcessItem(item))
            {
                _bag.Add(item);
            }
        }
    }

    private async Task<bool> ProcessItem(int item)
    {
        // Simulate processing time
        await Task.Delay(100);
        Console.WriteLine($"Processed {item} on thread {Task.CurrentId}");
        return true; // return your actual result here
    }
}

class EntryPoint
{

    public void Run()
    {
        var channel = Channel.CreateUnbounded<int>();
        var bag = new ConcurrentBag<int>();

        var producer = new Producer(channel.Writer);
        var consumers = Enumerable.Range(0, 10).Select(_ => new Consumer(channel.Reader, bag)).ToList();

        var producerTask = producer.ProduceAsync();
        var consumerTasks = consumers.Select(c => c.ConsumeAsync()).ToArray();

        Task.WaitAll(consumerTasks.Concat(new[] { producerTask }).ToArray());

        var json = JsonConvert.SerializeObject(bag);
        Console.WriteLine(json);
    }
}
