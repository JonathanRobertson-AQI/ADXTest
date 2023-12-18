using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ADXTest
{
    internal class JRADX
    {
        public async Task DoStuff()
        {
            JREvent model = new JREvent
            {
                ID = 3,
                Name = "Test"
            };

            string json = JsonConvert.SerializeObject(model);

            await Ingest(json, "https://ingest-oneadxfeature.eastus.kusto.windows.net", "JR", "JREvents");
        }


        public async Task Ingest(string jsonPayload, string ingestUri, string databaseName, string tableName)
        {
            // WithAadUserPromptAuthentication() prompts to login...
            // easy for proof of concept
            //KustoConnectionStringBuilder kustoConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication();

            string applicationId = "redacted";
            string password = "redacted";
            string tenant = "redacted";
            KustoConnectionStringBuilder kustoConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri).WithAadApplicationKeyAuthentication(applicationId, password, tenant);


            byte[] payloadBytes = Encoding.ASCII.GetBytes(jsonPayload);
            MemoryStream byteStream = new MemoryStream(payloadBytes);

            StreamSourceOptions sourceOptions = new StreamSourceOptions
            {
                Size = payloadBytes.Length,
                SourceId = Guid.NewGuid()
            };


            KustoIngestionProperties props = new KustoIngestionProperties
            {
                DatabaseName = databaseName,
                TableName = tableName,
                IngestionMapping = new IngestionMapping
                {
                    IngestionMappingReference = ""
                },
                Format = DataSourceFormat.multijson
            };
            props.AdditionalProperties.Add("OperationTimeoutMs", "60000");

            try
            {
                // should use one client per request
                //using (var client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilder))
                //{
                //    await client.IngestFromStreamAsync(byteStream, props, sourceOptions);
                //}

                using (var client = KustoIngestFactory.CreateDirectIngestClient(kustoConnectionStringBuilder))
                {
                    var result = await client.IngestFromStreamAsync(byteStream, props, sourceOptions);
                }

            }
            catch (Exception ex)
            {
                // catching exception for debugging
            }
        }
    }
}
