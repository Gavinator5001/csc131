using ConflictOfInterestDetector.Models;

namespace ConflictOfInterestDetector.Services
{
    public class ConflictMatcher
    {
        public List<ConflictCase> DetectConflicts(
            List<ConflictOfInterestDetector.Models.Official> officials,
            List<AgendaItem> agendaItems)
        {
            var conflicts = new List<ConflictCase>();

            foreach (var official in officials)
            {
                //if (official.Interests == null) continue; // skip officials with no interests

                foreach (var interest in official.Interests)
                {
                    foreach (var item in agendaItems)
                    {
                        if (item.Description.Contains(interest.CompanyName))
                        {
                            conflicts.Add(new ConflictCase
                            {
                                OfficialName = official.Name,
                                CompanyName = interest.CompanyName,
                                AgendaItemTitle = item.Title,
                                // DetectedDate = DateTime.Now
                            });
                        }
                    }
                }
            }
            return conflicts;
        }

        internal List<OfficialResult> SearchOfficials(string officialName, List<Official> officials)
        {
            throw new NotImplementedException();
        }
    }
}