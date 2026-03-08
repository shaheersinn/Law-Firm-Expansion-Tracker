"""Scraper registry — 28 scrapers."""

from scrapers.lateral_tracker  import LateralTrackScraper
from scrapers.deal_tracker     import DealTrackScraper
from scrapers.media            import MediaScraper
from scrapers.office_tracker   import OfficeTracker
from scrapers.recruiter        import RecruiterScraper
from scrapers.google_news      import GoogleNewsScraper
from scrapers.press            import PressScraper
from scrapers.publications     import PublicationsScraper
from scrapers.website          import WebsiteScraper
from scrapers.chambers         import ChambersScraper
from scrapers.awards           import AwardsScraper
from scrapers.barassoc         import BarAssociationScraper
from scrapers.jobs             import JobsScraper
from scrapers.lawschool        import LawSchoolScraper
from scrapers.rss              import RSSFeedScraper
from scrapers.govtrack         import GovTrackScraper
from scrapers.sedar            import SedarScraper
from scrapers.conference       import ConferenceScraper
from scrapers.lobbyist         import LobbyistScraper
from scrapers.canlii           import CanLIIScraper
from scrapers.linkedin         import LinkedInScraper
from scrapers.podcast          import PodcastScraper
from scrapers.alumni           import AlumniTrackScraper
from scrapers.thought_leader   import ThoughtLeaderScraper
from scrapers.diversity        import DiversityScraper
from scrapers.cipo_scraper     import CIPOScraper
from scrapers.event_scraper    import EventScraper
from scrapers.signal_cross_ref import SignalCrossRefScraper

__all__ = [
    "LateralTrackScraper", "DealTrackScraper", "MediaScraper", "OfficeTracker",
    "RecruiterScraper", "GoogleNewsScraper", "PressScraper", "PublicationsScraper",
    "WebsiteScraper", "ChambersScraper", "AwardsScraper", "BarAssociationScraper",
    "JobsScraper", "LawSchoolScraper", "RSSFeedScraper", "GovTrackScraper",
    "SedarScraper", "ConferenceScraper", "LobbyistScraper", "CanLIIScraper",
    "LinkedInScraper", "PodcastScraper", "AlumniTrackScraper", "ThoughtLeaderScraper",
    "DiversityScraper", "CIPOScraper", "EventScraper", "SignalCrossRefScraper",
]
