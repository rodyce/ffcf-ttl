namespace AllVersionsAndDeletes.Ttl
{
    using System;
    using System.Drawing.Text;
    using System.Net;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;
    
    enum RunMode
    {
        None,
        DataIngestion,
        DataRead,
    }

    class Program
    {
        private const string DatabaseName = "allversionsanddeletes-ttl-delete";
        private const string ContainerName = DatabaseName + "-container";
        private const int PageSizeHint = 10;
        private const int TtlTimeInSeconds = 30;
        private const int DocsToAdd = 50;
        static readonly Random random = new Random();


        static async Task Main(string[] args)
        {
            RunMode runMode = ParseArgs(args);

            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json")
                    .Build();

                string? endpoint = configuration["EndPointUrl"];
                if (string.IsNullOrEmpty(endpoint))
                {
                    throw new ArgumentNullException("Please specify a valid EndPointUrl in the appSettings.json");
                }

                string? authKey = configuration["AuthorizationKey"];
                if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
                {
                    throw new ArgumentException("Please specify a valid AuthorizationKey in the appSettings.json");
                }

                CosmosClientOptions clientOptions = new CosmosClientOptions { IsDistributedTracingEnabled = false, ConnectionMode = ConnectionMode.Gateway};

                using (CosmosClient client = new CosmosClient(endpoint, authKey, clientOptions))
                {
                    Console.WriteLine($"Getting container reference for {ContainerName}.");

                    ContainerProperties properties = new ContainerProperties(ContainerName, partitionKeyPath: "/id")
                    {
                        DefaultTimeToLive = -1, // Use "ttl" property in document
                    };
                    properties.ChangeFeedPolicy.FullFidelityRetention = TimeSpan.FromMinutes(10); // Enable all versions and deletes support

                    await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
                    ContainerResponse containerResponse = await client.GetDatabase(DatabaseName).CreateContainerIfNotExistsAsync(properties);
                    Container container = containerResponse.Container;

                    if(runMode == RunMode.DataIngestion)
                    {
                        await IngestData(
                            container, DocsToAdd, addTtl: true, ttlTimeInSeconds: TtlTimeInSeconds);
                    }
                    else if (runMode == RunMode.DataRead)
                    {
                        // Create a continuation token to start at the beginning of the retention period (LSN zero)
                        string allVersionsContinuationToken = GetContinuationTokenForContainer(containerResponse, startingLsn: 0);

                        Console.WriteLine("Reading collection with All Versions and Delete Change Feed");
                        await ReadAllVersionsAndDeletesChangeFeed(container, allVersionsContinuationToken, inspectChanges: true);
                    }
                }
            }
            finally
            {
                Console.WriteLine("End of demo.");
            }
        }

        static async Task<string> CreateAllVersionsAndDeletesChangeFeedIterator(Container container)
        {
            Console.WriteLine("Creating ChangeFeedIterator to read the change feed in All Versions and Deletes mode.");

            // <InitializeFeedIterator>
            using (FeedIterator<AllVersionsAndDeletesCFResponse> allVersionsIterator = container
                .GetChangeFeedIterator<AllVersionsAndDeletesCFResponse>(ChangeFeedStartFrom.Now(), ChangeFeedMode.AllVersionsAndDeletes))
            {
                while (allVersionsIterator.HasMoreResults)
                {
                    FeedResponse<AllVersionsAndDeletesCFResponse> response = await allVersionsIterator.ReadNextAsync();

                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        return response.ContinuationToken;
                    }
                }
            }
            // <InitializeFeedIterator>

            return string.Empty;
        }

        static async Task IngestData(Container container, int docsToAdd, bool addTtl, int ttlTimeInSeconds)
        {
            Console.WriteLine("Ingesting...");

            for (int i = 0; i < docsToAdd; i++)
            {
                Item item = GenerateItem();
                if(addTtl)
                {
                    item.TtlInSecs = ttlTimeInSeconds;
                }
                await container.UpsertItemAsync(item, new PartitionKey(item.Id));
                Console.Write("*");
            }
            Console.WriteLine("DONE");
        }

        private static Item GenerateItem()
        {
            return new Item
            {
                Id = Guid.NewGuid().ToString(),
                Value = random.Next(1, 100000),
            };
        }

        #region Change Feed
        static Task ReadAllVersionsAndDeletesChangeFeed(Container container, string allVersionsContinuationToken, bool inspectChanges = false)
        {
            return ReadChangeFeed(container, ChangeFeedMode.AllVersionsAndDeletes, allVersionsContinuationToken, inspectChanges);
        }

        static Task ReadIncrementalChangeFeed(Container container, string allVersionsContinuationToken, bool inspectChanges = false)
        {
            return ReadChangeFeed(container, ChangeFeedMode.LatestVersion, allVersionsContinuationToken, inspectChanges);
        }

        static async Task ReadChangeFeed(Container container, ChangeFeedMode changeFeedMode, string continuationToken, bool inspectChanges)
        {
            Console.WriteLine($"Reading in {changeFeedMode} mode. Press any key to stop.");

            // <ReadAllVersionsAndDeletesChanges>
            using (FeedIterator<AllVersionsAndDeletesCFResponse> allVersionsIterator = container.GetChangeFeedIterator<AllVersionsAndDeletesCFResponse>(
                ChangeFeedStartFrom.ContinuationToken(continuationToken),
                changeFeedMode,
                new ChangeFeedRequestOptions { PageSizeHint = PageSizeHint }))
            {
                int count = 0;

                while (allVersionsIterator.HasMoreResults)
                {
                    FeedResponse<AllVersionsAndDeletesCFResponse> response = await allVersionsIterator.ReadNextAsync();

                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        Console.WriteLine($"No more changes");
                        break;
                    }
                    
                    foreach (AllVersionsAndDeletesCFResponse r in response)
                    {
                        count++;

                        if (!inspectChanges) continue;

                        // if operation is delete
                        if (r.Metadata?.OperationType == "delete")
                        {
                            Item? item = r?.Previous;

                            if (r?.Metadata.TimeToLiveExpired == true)
                            {
                                Console.WriteLine($"Operation: {r.Metadata.OperationType} (due to TTL). Item id: {item?.Id}. Previous value: {item?.Value}");
                            }
                            else
                            {
                                Console.WriteLine($"Operation: {r?.Metadata.OperationType} (not due to TTL). Item id: {item?.Id}. Previous value: {item?.Value}");
                            }
                        }
                        // if operation is create or replace
                        else
                        {
                            Item? item = r?.Current;

                            Console.WriteLine($"Operation: {r?.Metadata?.OperationType}. Item id: {item?.Id}. Current value: {item?.Value}");
                        }
                    }
                }

                Console.WriteLine($"Found a total of {count} changes");
            }
            // <ReadAllVersionsAndDeletesChanges>
        }
        #endregion Change Feed

        #region Utility
        private static string GetContinuationTokenForContainer(ContainerResponse containerResponse, long startingLsn)
        {
            string containerResourceId = GetContainerResourceId(containerResponse.Resource.SelfLink);

            string continuationToken = $@"{{""V"":2,""Rid"":""{containerResourceId}"",""Continuation"":[{{""FeedRange"":{{""type"":""Effective Partition Key Range"",""value"":{{""min"":"""",""max"":""FF""}}}},""State"":{{""type"":""continuation"",""value"":""\""{startingLsn}\""""}}}}]}}";

            return continuationToken;
        }

        private static string GetContainerResourceId(string resourceSelfLink)
        {
            // dbs/db_resourceid/colls/coll_resourceid
            string pattern = "dbs/.*/colls/(?<containerResourceId>.*)/.*";
            Match match = Regex.Match(resourceSelfLink, pattern);
            string containerResourceId = match.Groups["containerResourceId"].Value;

            return containerResourceId;
        }

        private static RunMode ParseArgs(string[] args)
        {
            string errMsg = "Please specify INGEST for data ingestion or READ for reading";
            if (args.Length == 0)
            {
                Console.Error.WriteLine(errMsg);
                Environment.Exit(1);
                return RunMode.None;
            }

            string mode = args[0].ToUpper();
            if(mode == "INGEST")
            {
                return RunMode.DataIngestion;
            }
            else if(mode == "READ")
            {
                return RunMode.DataRead;
            }

            Console.Error.WriteLine(errMsg);
            Environment.Exit(1);
            return RunMode.None;
        }
        #endregion Utility

    }

    #region Data contract
    internal class AllVersionsAndDeletesCFResponse
    {
        [JsonProperty("current")]
        public Item? Current { get; set; }

        [JsonProperty("previous")]
        public Item? Previous { get; set; }

        [JsonProperty("metadata")]
        public Metadata? Metadata { get; set; }
    }

    internal class Item
    {
        [JsonProperty("id")]
        public string? Id { get; set; }

        public double Value { get; set; }

        [JsonProperty("ttl")]
        public int TtlInSecs { get; set; }
    }

    internal class Metadata
    {
        [JsonProperty("operationType")]
        public string? OperationType { get; set; }

        [JsonProperty("timeToLiveExpired")]
        public Boolean TimeToLiveExpired { get; set; }
    }
    #endregion Data contract
}