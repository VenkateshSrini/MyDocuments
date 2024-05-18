using ChoETL;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
namespace StringBuilderToMemStream;
public abstract class BaseClass
{
    protected BaseClass(string name)
    {
        Name = name;
    }

    public string Name { get; set; }
}

public class ChildClass : BaseClass
{
    //public ChildClass(string name) : base(name)
    //{
    //    Age = 0;
    //}
    public ChildClass(string name, int age) : base(name)
    {
        Age = age;
    }
    public int Age { get; set; }
}
public class ChildClass1 : BaseClass
{
    //public ChildClass(string name) : base(name)
    //{
    //    Age = 0;
    //}
    public ChildClass1(string name, int age, string gender) : base(name)
    {
        Age = age;
        Gender = gender;
    }
    public int Age { get; set; }
    public string Gender { get; set; }
}

public class BaseClassCollection : Dictionary<string, BaseClass>
{
    public BaseClassCollection() : base() { }
    public BaseClassCollection(IDictionary<string, BaseClass> dictionary) : base(dictionary) { }
}


public class BaseClassFormatter<T> : IMessagePackFormatter<T>
{

    public T Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        var dictionary = options.Resolver.GetFormatterWithVerify<Dictionary<string, object>>().Deserialize(ref reader, options);

        var type = Type.GetType((string)dictionary["Type"]);
        //var instance = Activator.CreateInstance(type, dictionary["Name"]);
        var instance = RuntimeHelpers.GetUninitializedObject(type);

        foreach (var prop in type.GetProperties().Where(p => p.CanWrite))
        {
            if (dictionary.ContainsKey(prop.Name))
            {
                prop.SetValue(instance, Convert.ChangeType(dictionary[prop.Name], prop.PropertyType));
            }
        }

        return (T)instance;
    }

    

    public void Serialize(ref MessagePackWriter writer, T value, MessagePackSerializerOptions options)
    {
        var dictionary = new Dictionary<string, object>
        {
            ["Type"] = value.GetType().AssemblyQualifiedName
        };

        foreach (var prop in value.GetType().GetProperties())
        {
            dictionary[prop.Name] = prop.GetValue(value);
        }

        options.Resolver.GetFormatterWithVerify<Dictionary<string, object>>().Serialize(ref writer, dictionary, options);
    }

   
}

public class BaseClassCollectionFormatter : IMessagePackFormatter<BaseClassCollection>
{

    public BaseClassCollection Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        var formatter = options.Resolver.GetFormatterWithVerify<Dictionary<string, BaseClass>>();
        var dictionary = formatter.Deserialize(ref reader, options);
        return new BaseClassCollection(dictionary);

    }



    public void Serialize(ref MessagePackWriter writer, BaseClassCollection value, MessagePackSerializerOptions options)
    {
        var formatter = options.Resolver.GetFormatterWithVerify<Dictionary<string, BaseClass>>();
        formatter.Serialize(ref writer, value, options);
    }
}
public class SimpleCollection : Dictionary<string, decimal>
{
    // Add any additional properties or methods here.
}
public class SimpleCollectionFormatter : IMessagePackFormatter<SimpleCollection>
{
    public SimpleCollection Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
        {
            return null;
        }

        var length = reader.ReadMapHeader();
        var collection = new SimpleCollection();

        for (int i = 0; i < length; i++)
        {
            var key = reader.ReadString();
            var value = Convert.ToDecimal(reader.ReadString());
            collection.Add(key, value);
        }

        return collection;
    }

    public void Serialize(ref MessagePackWriter writer, SimpleCollection value, MessagePackSerializerOptions options)
    {
        writer.WriteMapHeader(value.Count);

        foreach (var kvp in value)
        {
            writer.Write(kvp.Key);
            writer.Write(kvp.Value.ToString());
        }
    }
}
public class MPBaseEntrypoint
{
    public static void Main()
    {
        var baseClassCollection = new BaseClassCollection
        {
            { "Child1", new ChildClass("Child1", 10) },
            { "Child2", new ChildClass1("Child2", 20,"male") }
        };

        var dictionary = new Dictionary<int, BaseClassCollection>
        {
            { 1, baseClassCollection }
        };

        var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
        {
            new BaseClassFormatter<BaseClass>(),
            new BaseClassCollectionFormatter()
        }, new[] { StandardResolver.Instance }));

        using var stream = new MemoryStream();
        var pipeWriter = PipeWriter.Create(stream);
        
        var writer = new MessagePackWriter(pipeWriter);
        options.Resolver.GetFormatterWithVerify<Dictionary<int, BaseClassCollection>>().Serialize(ref writer, dictionary, options);
        writer.Flush();
        pipeWriter.FlushAsync().GetAwaiter().GetResult();

        stream.Position = 0;
        var sequence = new ReadOnlySequence<byte>(stream.ToArray());

        var reader = new MessagePackReader(sequence);
        var deserialized = options.Resolver.GetFormatterWithVerify<Dictionary<int, BaseClassCollection>>().Deserialize(ref reader, options);


        Console.WriteLine("Deserialized: ");
        foreach (var kvp in deserialized)
        {
            Console.WriteLine($"Key: {kvp.Key}");
            foreach (var baseClass in kvp.Value.Values)
            {
                Console.WriteLine(baseClass.Name);
                if (baseClass is ChildClass child)
                {
                    Console.WriteLine(child.Age);
                }
                if (baseClass is ChildClass1 child1)
                {
                    Console.WriteLine(child1.Age);
                    Console.WriteLine(child1.Gender);
                }
            }
        }
    }
    public static void Run()
    {
        var baseClassCollection = new BaseClassCollection
        {
            { "Child1", new ChildClass("Child1", 10) },
            { "Child2", new ChildClass1("Child2", 20,"male") }
        };


        var dictionary = new Dictionary<int, BaseClassCollection>
        {
            { 1, baseClassCollection }
        };

        var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
        {
            new BaseClassFormatter<BaseClass>(),
            new BaseClassCollectionFormatter()
        }, new[] { StandardResolver.Instance }));

        var data = MessagePackData.FromObject(dictionary, options);
        var deserialized = data.ToObject<Dictionary<int, BaseClassCollection>>(options);

        Console.WriteLine("Deserialized: ");
        foreach (var kvp in deserialized)
        {
            Console.WriteLine($"Key: {kvp.Key}");
            foreach (var baseClass in kvp.Value.Values)
            {
                Console.WriteLine(baseClass.Name);
                if (baseClass is ChildClass child)
                {
                    Console.WriteLine(child.Age);
                }
                if (baseClass is ChildClass1 child1)
                {
                    Console.WriteLine(child1.Age);
                    Console.WriteLine(child1.Gender);
                }
            }
        }
    }
    public static void SimpleSerializer()
    {
        var simpleCollection = new SimpleCollection
        {
            { "Item1", 10.5m },
            { "Item2", 20.5m }
        };

        var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
        {
            new SimpleCollectionFormatter()
        }, new[] { StandardResolver.Instance }));

        var data = MessagePackData.FromObject(simpleCollection, options);
        var deserialized = data.ToObject<SimpleCollection>(options);

        Console.WriteLine("Deserialized: ");
        foreach (var kvp in deserialized)
        {
            Console.WriteLine($"Key: {kvp.Key}, Value: {kvp.Value}");
        }
    }
}