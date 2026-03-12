using Microsoft.EntityFrameworkCore;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<Official> Officials { get; set; }
    public DbSet<ConflictCase> Conflicts { get; set; }

}

public class Official
{
    public string? Interests { get; set; }

    public string? Id { get; set; }
    public string? Name { get; set; }
    public string? CompanyName { get; set; }
}

public class ConflictCase
{
    public string? OfficialName { get; set; }
    public string? CompanyName { get; set; }
    public string? AgendaItemTitle { get; set; }
    }