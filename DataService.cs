using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using IBM.Data.DB2.Core;

public class DataService
{
    private readonly string _db2ConnectionString;
    private readonly string _imsConnectionString;

    public DataService(string db2ConnectionString, string imsConnectionString)
    {
        _db2ConnectionString = db2ConnectionString;
        _imsConnectionString = imsConnectionString;
    }

    public async Task<List<ClaimInfo>> GetClaimInfosAsync(string[] claimIds)
    {
        var claimInfos = new List<ClaimInfo>();

        // BroadcastBlock to distribute claim IDs to both DB2 and IMS blocks
        var broadcastBlock = new BroadcastBlock<string>(claimId => claimId);

        // TransformBlock to fetch data from DB2
        var db2Block = new TransformBlock<string, (string ClaimId, string DB2Data)>(async claimId =>
        {
            using (var connection = new DB2Connection(_db2ConnectionString))
            {
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = "SELECT * FROM Claims WHERE ClaimId = @ClaimId";
                command.Parameters.Add(new DB2Parameter("@ClaimId", claimId));

                string db2Data = null;
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        db2Data = reader["DataColumn"].ToString();
                    }
                }
                return (claimId, db2Data);
            }
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 10 });

        // TransformBlock to fetch data from IMS
        var imsBlock = new TransformBlock<string, (string ClaimId, string IMSData)>(async claimId =>
        {
            // Implement your IMS Connect data retrieval logic here
            // This is a placeholder for the actual implementation
            await Task.Delay(100); // Simulate async work
            return (claimId, "IMS Data for " + claimId);
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 10 });

        // BufferBlock to temporarily store DB2 results
        var db2Buffer = new BufferBlock<(string ClaimId, string DB2Data)>();

        // BufferBlock to temporarily store IMS results
        var imsBuffer = new BufferBlock<(string ClaimId, string IMSData)>();

        // JoinBlock to pair the results from DB2 and IMS
        var joinBlock = new JoinBlock<(string ClaimId, string DB2Data), (string ClaimId, string IMSData)>();

        // ActionBlock to process the joined results
        var finalBlock = new ActionBlock<Tuple<(string ClaimId, string DB2Data), (string ClaimId, string IMSData)>>(data =>
        {
            var claimInfo = new ClaimInfo
            {
                ClaimId = data.Item1.ClaimId,
                DB2Data = data.Item1.DB2Data,
                IMSData = data.Item2.IMSData
            };
            lock (claimInfos)
            {
                claimInfos.Add(claimInfo);
            }
        });

        // Link the blocks
        broadcastBlock.LinkTo(db2Block);
        broadcastBlock.LinkTo(imsBlock);
        db2Block.LinkTo(db2Buffer);
        imsBlock.LinkTo(imsBuffer);
        db2Buffer.LinkTo(joinBlock.Target1);
        imsBuffer.LinkTo(joinBlock.Target2);
        joinBlock.LinkTo(finalBlock);

        // Post the claim IDs to the broadcast block
        foreach (var claimId in claimIds)
        {
            broadcastBlock.Post(claimId);
        }

        // Complete the blocks
        broadcastBlock.Complete();
        await Task.WhenAll(db2Block.Completion, imsBlock.Completion);
        db2Buffer.Complete();
        imsBuffer.Complete();
        joinBlock.Complete();
        await finalBlock.Completion;

        return claimInfos;
    }
}
