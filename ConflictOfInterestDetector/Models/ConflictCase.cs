namespace ConflictOfInterestDetector.Models
{
    public class ConflictCase
    {
        public int Id { get; set; }
        public string? OfficialName { get; set; }
        public string? CompanyName { get; set; }
        public string? AgendaItemTitle { get; set; }
        public DateTime DetectedDate { get; set; }
    }
}