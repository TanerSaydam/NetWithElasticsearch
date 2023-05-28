using Microsoft.EntityFrameworkCore;

namespace NetWithElasticsearch.Context;

public sealed class AppDbContext : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer("Data Source=DESKTOP-3BJ5GK9;Initial Catalog=TestDb;Integrated Security=True;Connect Timeout=30;Encrypt=False;Trust Server Certificate=False;Application Intent=ReadWrite;Multi Subnet Failover=False");
    }

    public DbSet<Travel> Travels { get; set; }   
}

public sealed class Travel
{
    public int Id { get; set; }
    public string Title { get; set; }
    public string Description { get; set; }
}
