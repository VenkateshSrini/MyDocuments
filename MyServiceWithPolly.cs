using Polly;
using System;
using System.Data.SqlClient;

public class MyService
{
    public void DoSomething()
    {
        // Define your retry policy
        var retryPolicy = Policy
            .Handle<SqlException>(ex => ex.InnerException is TimeoutException)
            .WaitAndRetry(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

        // Use the policy
        retryPolicy.Execute(() =>
        {
            using (var connection = new SqlConnection("YourConnectionString"))
            {
                // Perform your database operation...
            }
        });
    }
}
