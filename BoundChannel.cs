using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace StringBuilderToMemStream
{
    internal class BoundChannel
    {
        var channel = Channel.CreateBounded<DataTable>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        // Assume GetDataReader is a function that returns an IDataReader.
        IDataReader reader = GetDataReader();

        // Writer task
        Task writerTask = Task.Run(() =>
        {
            DataTable dt = new DataTable();
            while (reader.Read())
            {
                DataRow row = dt.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.GetValue(i);
                }
                dt.Rows.Add(row);

                if (dt.Rows.Count > 199)
                {
                    channel.Writer.TryWrite(dt);
                    dt = new DataTable(); // Create a new DataTable for the next batch of rows.
                }
            }

            // Don't forget to complete the writer when you're done adding items.
            channel.Writer.Complete();
        });

        // Reader task
        Task readerTask = Task.Run(async () =>
        {
            await foreach (var dt in channel.Reader.ReadAllAsync())
            {
                // Assume SqlBulkCopy is a function that performs the SQL Bulk Copy operation.
                SqlBulkCopy(dt);
            }
        });

        // Wait for both tasks to complete
        Task.WaitAll(writerTask, readerTask);

    }
}
