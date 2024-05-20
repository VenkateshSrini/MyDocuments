using ChoETL;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
namespace StringBuilderToMemStream;
public abstract class BaseClass
{
    protected BaseClass(string name)
    {
        Name = name;
    }

    public string Name { get; set; }
    protected Dictionary<short, string> Properties { get; set; }
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
        Properties = new Dictionary<short, string>();
        Properties.Add(1, "childclass");
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
        Properties = new Dictionary<short, string>();
        Properties.Add(1, "childclass1");
    }
    public int Age { get; set; }
    public string Gender { get; set; }
    
}

//public class BaseClassCollection : Dictionary<string, BaseClass>
//{
//    public BaseClassCollection() : base() { }
//    public BaseClassCollection(IDictionary<string, BaseClass> dictionary) : base(dictionary) { }
//}

public class Composer
{
    private Dictionary<short, BaseClass> baseClasses;
    private string Name { get; set; }
    public Dictionary<short, BaseClass> Collection
    {
        get { return baseClasses; }
        set { baseClasses = value; }
      
    }
    public Composer()
    {
        Name = "abc";
        baseClasses =new Dictionary<short, BaseClass>();
    }
        
}

public class AbstractClassFormatter<T> : IMessagePackFormatter<T>
{

    public T Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        var dictionary = options.Resolver.GetFormatterWithVerify<Dictionary<string, object>>().Deserialize(ref reader, options);

        var type = Type.GetType((string)dictionary["Type"]);
        //var instance = Activator.CreateInstance(type, dictionary["Name"]);
        var instance = RuntimeHelpers.GetUninitializedObject(type);

        foreach (var prop in type.GetProperties(BindingFlags.Public|BindingFlags.NonPublic | BindingFlags.Instance).Where(p => p.CanWrite))
        {
            if (dictionary.ContainsKey(prop.Name))
            {
                if (prop.PropertyType == typeof(Dictionary<short, string>))
                {
                    var objectDictionary = dictionary[prop.Name] as Dictionary<object, object>;
                    var shortStringDictionary = objectDictionary.ToDictionary(k => Convert.ToInt16(k.Key), v => v.Value.ToString());
                    prop.SetValue(instance, shortStringDictionary);
                }
                else
                {
                    prop.SetValue(instance, Convert.ChangeType(dictionary[prop.Name], prop.PropertyType));
                }
            }
        }
        foreach (var field in type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
        {
            if (dictionary.ContainsKey(field.Name))
            {
                field.SetValue(instance, Convert.ChangeType(dictionary[field.Name], field.FieldType));
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

        foreach (var prop in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
        {
            dictionary[prop.Name] = prop.GetValue(value);
        }
        foreach (var field in value.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
        {
            dictionary[field.Name] = field.GetValue(value);
        }

            options.Resolver.GetFormatterWithVerify<Dictionary<string, object>>().Serialize(ref writer, dictionary, options);
    }

   
}

//public class BaseClassCollectionFormatter : IMessagePackFormatter<BaseClassCollection>
//{

//    public BaseClassCollection Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
//    {
//        var formatter = options.Resolver.GetFormatterWithVerify<Dictionary<string, BaseClass>>();
//        var dictionary = formatter.Deserialize(ref reader, options);
//        return new BaseClassCollection(dictionary);

//    }



//    public void Serialize(ref MessagePackWriter writer, BaseClassCollection value, MessagePackSerializerOptions options)
//    {
//        var formatter = options.Resolver.GetFormatterWithVerify<Dictionary<string, BaseClass>>();
//        formatter.Serialize(ref writer, value, options);
//    }
//}
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
public class ShortBaseClassDictionaryFormatter<TKey,TBaseClass> : IMessagePackFormatter<Dictionary<TKey, TBaseClass>>
{
    // Use built-in formatters for keys and your custom formatter for values
    private readonly IMessagePackFormatter<TKey> keyFormatter = MessagePackSerializerOptions.Standard.Resolver.GetFormatterWithVerify<TKey>();
    private readonly IMessagePackFormatter<TBaseClass> valueFormatter = new AbstractClassFormatter<TBaseClass>();

    public Dictionary<TKey, TBaseClass> Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
        {
            return null;
        }

        var length = reader.ReadMapHeader();
        var dictionary = new Dictionary<TKey, TBaseClass>(length);

        for (int i = 0; i < length; i++)
        {
            var key = keyFormatter.Deserialize(ref reader, options);
            var value = valueFormatter.Deserialize(ref reader, options);
            dictionary.Add(key, value);
        }

        return dictionary;
    }

    public void Serialize(ref MessagePackWriter writer, Dictionary<TKey, TBaseClass> value, MessagePackSerializerOptions options)
    {
        writer.WriteMapHeader(value.Count);

        foreach (var kvp in value)
        {
            keyFormatter.Serialize(ref writer, kvp.Key, options);
            valueFormatter.Serialize(ref writer, kvp.Value, options);
        }
    }
}

public class ComposerFormatter : IMessagePackFormatter<Composer>
{
    private readonly IMessagePackFormatter<Dictionary<short, BaseClass>> formatter = new ShortBaseClassDictionaryFormatter<short,BaseClass>();
    private readonly IMessagePackFormatter<string> stringFormatter = MessagePackSerializerOptions.Standard.Resolver.GetFormatterWithVerify<string>();

    public Composer Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
        {
            return null;
        }

        var length = reader.ReadMapHeader();
        var composer = new Composer();

        for (int i = 0; i < length; i++)
        {
            var propertyName = reader.ReadString();

            switch (propertyName)
            {
                case nameof(Composer.Collection):
                    composer.Collection = formatter.Deserialize(ref reader, options);
                    break;
                case "Name":
                    var nameProperty = typeof(Composer).GetProperty("Name", BindingFlags.NonPublic | BindingFlags.Instance);
                    if (nameProperty != null)
                    {
                        var nameValue = stringFormatter.Deserialize(ref reader, options);
                        nameProperty.SetValue(composer, nameValue);
                    }
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        return composer;
    }

    public void Serialize(ref MessagePackWriter writer, Composer value, MessagePackSerializerOptions options)
    {
        writer.WriteMapHeader(2); // 2 properties to serialize: Collection and Name

        writer.Write(nameof(Composer.Collection));
        formatter.Serialize(ref writer, value.Collection, options);

        // Serialize private property
        var nameProperty = typeof(Composer).GetProperty("Name", BindingFlags.NonPublic | BindingFlags.Instance);
        if (nameProperty != null)
        {
            var nameValue = (string)nameProperty.GetValue(value);
            writer.Write("Name");
            stringFormatter.Serialize(ref writer, nameValue, options);
        }
    }
}


public class MPBaseEntrypoint
{
    //public static void Main()
    //{
    //    var baseClassCollection = new BaseClassCollection
    //    {
    //        { "Child1", new ChildClass("Child1", 10) },
    //        { "Child2", new ChildClass1("Child2", 20,"male") }
    //    };

    //    var dictionary = new Dictionary<int, BaseClassCollection>
    //    {
    //        { 1, baseClassCollection }
    //    };

    //    var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
    //    {
    //        new AbstractClassFormatter<BaseClass>(),
    //        new BaseClassCollectionFormatter()
    //    }, new[] { StandardResolver.Instance }));

    //    using var stream = new MemoryStream();
    //    var pipeWriter = PipeWriter.Create(stream);
        
    //    var writer = new MessagePackWriter(pipeWriter);
    //    options.Resolver.GetFormatterWithVerify<Dictionary<int, BaseClassCollection>>().Serialize(ref writer, dictionary, options);
    //    writer.Flush();
    //    pipeWriter.FlushAsync().GetAwaiter().GetResult();

    //    stream.Position = 0;
    //    var sequence = new ReadOnlySequence<byte>(stream.ToArray());

    //    var reader = new MessagePackReader(sequence);
    //    var deserialized = options.Resolver.GetFormatterWithVerify<Dictionary<int, BaseClassCollection>>().Deserialize(ref reader, options);


    //    Console.WriteLine("Deserialized: ");
    //    foreach (var kvp in deserialized)
    //    {
    //        Console.WriteLine($"Key: {kvp.Key}");
    //        foreach (var baseClass in kvp.Value.Values)
    //        {
    //            Console.WriteLine(baseClass.Name);
    //            if (baseClass is ChildClass child)
    //            {
    //                Console.WriteLine(child.Age);
    //            }
    //            if (baseClass is ChildClass1 child1)
    //            {
    //                Console.WriteLine(child1.Age);
    //                Console.WriteLine(child1.Gender);
    //            }
    //        }
    //    }
    //}
    //public static void Run()
    //{
    //    var baseClassCollection = new BaseClassCollection
    //    {
    //        { "Child1", new ChildClass("Child1", 10) },
    //        { "Child2", new ChildClass1("Child2", 20,"male") }
    //    };


    //    var dictionary = new Dictionary<int, BaseClassCollection>
    //    {
    //        { 1, baseClassCollection }
    //    };

    //    var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
    //    {
    //        new AbstractClassFormatter<BaseClass>(),
    //        new BaseClassCollectionFormatter()
    //    }, new[] { StandardResolver.Instance })).WithCompression(MessagePackCompression.Lz4BlockArray);


    //    var data = MessagePackData.FromObject(dictionary, options);
    //    var deserialized = data.ToObject<Dictionary<int, BaseClassCollection>>(options);

    //    Console.WriteLine("Deserialized: ");
    //    foreach (var kvp in deserialized)
    //    {
    //        Console.WriteLine($"Key: {kvp.Key}");
    //        foreach (var baseClass in kvp.Value.Values)
    //        {
    //            Console.WriteLine(baseClass.Name);
    //            if (baseClass is ChildClass child)
    //            {
    //                Console.WriteLine(child.Age);
                    
                        
    //            }
    //            if (baseClass is ChildClass1 child1)
    //            {
    //                Console.WriteLine(child1.Age);
    //                Console.WriteLine(child1.Gender);
                    
    //            }
    //        }
    //    }
    //}
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
    public static void RunComposer()
    {
        var composer = new Composer();
        composer.Collection = new Dictionary<short, BaseClass>
        {
            { 1, new ChildClass("Child1", 10) },
            { 2, new ChildClass1("Child2", 20,"male") }
        };
        var options = MessagePackSerializerOptions.Standard.WithResolver(CompositeResolver.Create(new IMessagePackFormatter[]
        {
           //new AbstractClassFormatter<BaseClass>(),
           new ComposerFormatter()
        
           
        }, new[] { StandardResolver.Instance })).WithCompression(MessagePackCompression.Lz4BlockArray);


        var data = MessagePackData.FromObject(composer, options);
        var deserialized = data.ToObject<Composer>(options);
    }
}