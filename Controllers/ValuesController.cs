using Elasticsearch.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using NetWithElasticsearch.Context;
using NetWithElasticsearch.Generics;
using Newtonsoft.Json.Linq;
using System.Reflection.Metadata.Ecma335;

namespace NetWithElasticsearch.Controllers;

[Route("api/[controller]")]
[ApiController]
public sealed class ValuesController : ControllerBase
{
    AppDbContext context = new();

    [HttpGet("[action]")]
    public async Task<IActionResult> CreateData(CancellationToken cancellationToken)
    {
        IList<Travel> travels = new List<Travel>(); 
        var random = new Random();

        for (int i = 0; i < 10000; i++)
        {
            var title = new string(Enumerable.Repeat("abcdefgğhıijklmnoöprsştuwyz",5)
                .Select(s=> s[random.Next(s.Length)]).ToArray());

            var words = new List<string>();
            for (int j = 0; j < 500; j++)
            {
                words.Add(new string(Enumerable.Repeat("abcdefgğhıijklmnoöprsştuwyz", 5)
                .Select(s => s[random.Next(s.Length)]).ToArray()));
            }

            var description = string.Join(" ", words);
            var traver = new Travel()
            {
                Title = title,
                Description = description,
            };

            travels.Add(traver);
        }

        await context.Set<Travel>().AddRangeAsync(travels, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);

        return Ok();
    }

    [HttpGet("[action]/{value}")]
    public async Task<IActionResult> GetDataListWithEF(string value)
    {
        IList<Travel> travels = 
            await context.Set<Travel>()
            .Where(p => p.Description.Contains(value))
            .AsNoTracking()
            .ToListAsync();

        return Ok(travels.Take(10));
    }

    [HttpGet("[action]")]
    public async Task<IActionResult> SyncToElastic()
    {
        var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"));

        var client = new ElasticLowLevelClient(settings);

        List<Travel> travels = await context.Travels.ToListAsync();

        var tasks = new List<Task>();

        foreach (var travel in travels)
        {
            //tasks.Add(client.IndexAsync<StringResponse>("travels", travel.Id.ToString(), PostData.Serializable(new
            //{
            //    travel.Id,
            //    travel.Title,
            //    travel.Description
            //})));

            var response = await client.GetAsync<StringResponse>("travels", travel.Id.ToString());

            if (response.HttpStatusCode != 200)
            {
                tasks.Add(client.IndexAsync<StringResponse>("travels", travel.Id.ToString(), PostData.Serializable(travel)));
            }
        }

        await Task.WhenAll(tasks);

        return Ok();
    }

    [HttpGet("[action]")]
    public async Task<IActionResult> SyncToElasticWithService()
    {
        await ElasticsearchService.SyncToElastic<Travel>("travels", () => context.Travels.ToListAsync());

        return Ok();
    }
    
    [HttpGet("[action]")]
    public async Task<IActionResult> SyncSingleToElasticWithService()
    {
        Travel travel = new();
        await ElasticsearchService.SyncSingleToElastic<Travel>("travels", travel);

        return Ok();
    }

    [HttpGet("[action]/{value}")]
    public async Task<IActionResult> GetDataListWithElasticSearch(string value)
    {
        var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"));

        var client = new ElasticLowLevelClient(settings);

        var response = await client.SearchAsync<StringResponse>("travels", PostData.Serializable(new
        {
            query = new
            {
                wildcard = new
                {
                    Description = new { value = $"*{value}*" }
                }
            }
        }));

        var results = JObject.Parse(response.Body);

        var hits = results["hits"]["hits"].ToObject<List<JObject>>();

        List<Travel> travels = new();

        foreach (var hit in hits)
        {
            travels.Add(hit["_source"].ToObject<Travel>());
        }

        return Ok(travels.Take(10));
    }

    [HttpGet("[action]/{value}")]
    public async Task<IActionResult> GetDataListWithElasticSearchToService(string value)
    {
        var response = await ElasticsearchService.GetDataListWithElasticSearch<Travel>("travels", "Description", value);

        return Ok(response.Take(10));
    }

    //Bu metod, Sql Server Full Text Search özelliği kurulu ise çalışabilir
    [HttpGet("[action]/{value}")]
    public async Task<IActionResult> GetDataListWithFullText(string value)
    {
        IList<Travel> travels =
            await context.Set<Travel>()
            .Where(p => EF.Functions.Contains(p.Description, value))
            .AsNoTracking()
            .ToListAsync();

        return Ok(travels.Take(10));
    }
}

