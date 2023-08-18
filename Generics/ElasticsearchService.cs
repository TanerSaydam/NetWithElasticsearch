using Elasticsearch.Net;
using Newtonsoft.Json.Linq;

namespace NetWithElasticsearch.Generics;

public static class ElasticsearchService
{
    private static ElasticLowLevelClient GetClient()
    {
        var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"));
        var client = new ElasticLowLevelClient(settings);
        return client;
    }
    public static async Task SyncToElastic<T>(string indexName, Func<Task<List<T>>> getDataFunc) where T : class
    {
        var client = GetClient();

        List<T> items = await getDataFunc();

        var tasks = new List<Task>();

        foreach (var item in items)
        {
            var itemId = item.GetType().GetProperty("Id")?.GetValue(item).ToString();

            if (string.IsNullOrEmpty(itemId))
            {
                // Hata işlemesi eklenebilir
                continue;
            }

            var response = await client.GetAsync<StringResponse>(indexName, itemId);

            if (response.HttpStatusCode != 200)
            {
                tasks.Add(client.IndexAsync<StringResponse>(indexName, itemId, PostData.Serializable(item)));
            }
        }

        await Task.WhenAll(tasks);
    }

    public static async Task SyncSingleToElastic<T>(string indexName, T data) where T : class
    {
        var client = GetClient();

        var dataId = data.GetType().GetProperty("Id")?.GetValue(data).ToString();

        if (string.IsNullOrEmpty(dataId))
        {
            throw new Exception("Id bulunamadı!"); // Veya uygun başka bir yanıt
        }

        var response = await client.GetAsync<StringResponse>(indexName, dataId);

        if (response.HttpStatusCode != 200)
        {
            await client.IndexAsync<StringResponse>(indexName, dataId, PostData.Serializable(data));
        }
    }

    public static async Task<List<T>> GetDataListWithElasticSearch<T>(string indexName, string fieldName, string value) where T : class
    {
        var client = GetClient();

        var searchQuery = new
        {
            query = new
            {
                wildcard = new Dictionary<string, object>
            {
                { fieldName, new { value = $"*{value}*" } }
            }
            }
        };

        var response = await client.SearchAsync<StringResponse>(indexName, PostData.Serializable(searchQuery));

        var results = JObject.Parse(response.Body);

        var hits = results["hits"]["hits"].ToObject<List<JObject>>();

        List<T> items = new();

        foreach (var hit in hits)
        {
            items.Add(hit["_source"].ToObject<T>());
        }

        return items;
    }
}
