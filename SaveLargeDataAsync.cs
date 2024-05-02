public async Task SaveLargeDataAsync(string key, Stream dataStream)

{

    const int bufferSize = 4096; // Adjust this size as needed

    byte[] buffer = new byte[bufferSize];

    int bytesRead;

    long offset = 0;

    int batchCount = 0;
 
    using (SqlConnection connection = new SqlConnection("your_connection_string"))

    {

        await connection.OpenAsync();
 
        // Start a new transaction

        using (SqlTransaction transaction = connection.BeginTransaction())

        {

            // Insert an empty array first to create the row

            using (SqlCommand command = new SqlCommand("INSERT INTO Cache (key, cachedata) VALUES (@Key, 0x)", connection, transaction))

            {

                command.Parameters.AddWithValue("@Key", key);

                await command.ExecuteNonQueryAsync();

            }
 
            // Create a DataTable to hold the updates

            DataTable updates = new DataTable();

            updates.Columns.Add("Key", typeof(string));

            updates.Columns.Add("Data", typeof(byte[]));

            updates.Columns.Add("Offset", typeof(long));
 
            // Read the data in chunks and add the updates to the DataTable

            while ((bytesRead = await dataStream.ReadAsync(buffer, 0, buffer.Length)) > 0)

            {

                updates.Rows.Add(key, buffer.Take(bytesRead).ToArray(), offset);

                offset += bytesRead;

                batchCount++;
 
                // If we've reached the batch size, update the database and clear the DataTable

                if (batchCount >= 50)

                {

                    await UpdateDatabaseAsync(connection, transaction, updates);

                    updates.Clear();

                    batchCount = 0;

                }

            }
 
            // Update the database with any remaining updates

            if (updates.Rows.Count > 0)

            {

                await UpdateDatabaseAsync(connection, transaction, updates);

            }
 
            transaction.Commit();

        }

    }

}
 
private async Task UpdateDatabaseAsync(SqlConnection connection, SqlTransaction transaction, DataTable updates)

{

    // Use a SqlDataAdapter to batch the updates

    using (SqlCommand command = new SqlCommand("UPDATE Cache SET cachedata.WRITE(@Data, NULL, @Offset) WHERE key = @Key", connection, transaction))

    {

        command.Parameters.Add("@Key", SqlDbType.VarChar, 100, "Key");

        command.Parameters.Add("@Data", SqlDbType.VarBinary, -1, "Data");

        command.Parameters.Add("@Offset", SqlDbType.BigInt, 8, "Offset");
 
        using (SqlDataAdapter adapter = new SqlDataAdapter())

        {

            adapter.UpdateCommand = command;

            await adapter.UpdateAsync(updates);

        }

    }

}
