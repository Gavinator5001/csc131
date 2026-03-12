namespace ConflictOfInterestDetector.Models
{
    public class Official
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Position { get; set; }
        public List<CompanyInterest>? Interests { get; set; }
    }
}