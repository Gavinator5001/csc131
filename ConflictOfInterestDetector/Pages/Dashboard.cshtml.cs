using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using ConflictOfInterestDetector.Models;
using ConflictOfInterestDetector.Services;
using System.ComponentModel.DataAnnotations;
using MailKit;

public class DashboardModel : PageModel
{

    [BindProperty]
    public String? OfficialName { get; set; }
    public String? OfficialID { get; set; }
    public List<ConflictCase>? Conflicts { get; set; }
    public List<OfficialResult>? SearchResults { get; set; }

    public void OnPost()
    {
        var matcher = new ConflictMatcher();

        var officials = new List<Official>();

        // hard code testing
        /*var officials = new List<Official>
        {
            new Official
            {
                Name = "Emily Gray",
                Position = "Security Analyst",
                Interests = new List<CompanyInterest>
                {
                    new CompanyInterest
                    {
                        CompanyName = "TechCorp",
                        InterestType = "Stock Ownership"
                    }
                }
            }
        };*/

        var agendaItems = new List<AgendaItem>();

        // hard codde testing
        /*var agendaItems = new List<AgendaItem>
        {
            new AgendaItem
            {
                Title = "TechCorp Security Contract",
                Description = "Discussion on awarding a security contract to TechCorp.",
                MeetingDate = DateTime.Now
            }
        };*/


       // Conflicts = matcher.DetectConflicts(officials, agendaItems);

        // SearchResults = matcher.SearchOfficials(OfficialName, officials);
    }

    public void OnPostAnalyze() 
    {
        // Look up official by ID
        // var official = DataProtectionServiceCollectionExtensions.GetOfficialByID(OfficialID);

        //Run your conflict detection logic for that official and update the Conflicts property
        // Conflicts = _conflictService.FindConflictsForOfficial(official);

        // TODO
        // Send email notifications
        // MailService.SendAlert(Conflicts);


    }
}

public class OfficialResult
{
    public string? OfficialName { get; set; }
    public string? CompanyName { get; set; }
    public String? ID { get; set; } // unique official ID
}