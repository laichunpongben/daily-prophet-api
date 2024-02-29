import logging

import feedparser

from dailyprophet.feeds.feed import Feed


logger = logging.getLogger(__name__)


class ArxivFeed(Feed):
    def __init__(self, category):
        super().__init__()
        self.category = category
        self.feed_url = f"http://export.arxiv.org/rss/{category}"

    def parse(self, post):
        """
        {'title': 'APTQ: Attention-aware Post-Training Mixed-Precision Quantization for Large Language Models', 'title_detail': {'type': 'text/plain', 'language': None, 'base': 'https://rss.arxiv.org/rss/cs.LG',
        'value': 'APTQ: Attention-aware Post-Training Mixed-Precision Quantization for Large Language Models'}, 'links': [{'rel': 'alternate', 'type': 'text/html', 'href': 'https://arxiv.org/abs/2402.14866'}],
        'link': 'https://arxiv.org/abs/2402.14866', 'summary': "arXiv:2402.14866v1 Announce Type: new \nAbstract: Large Language Models (LLMs) ...... quantized LLMs.",
        'summary_detail': {'type': 'text/html', 'language': None, 'base': 'https://rss.arxiv.org/rss/cs.LG',
        'value': "arXiv:2402.14866v1 Announce Type: new \nAbstract: Large Language Models (LLMs) ...... quantized LLMs."}, 'id': 'oai:arXiv.org:2402.14866v1', 'guidislink': False,
        'tags': [{'term': 'cs.LG', 'scheme': None, 'label': None}, {'term': 'cs.AI', 'scheme': None, 'label': None}, {'term': 'cs.CL', 'scheme': None, 'label': None}], 'arxiv_announce_type': 'new',
        'rights': 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/', 'rights_detail': {'type': 'text/plain', 'language': None, 'base': 'https://rss.arxiv.org/rss/cs.LG',
        'value': 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/'}, 'authors': [{'name': 'Ziyi Guan, Hantao Huang, Yupeng Su, Hong Huang, Ngai Wong, Hao Yu'}],
        'author': 'Ziyi Guan, Hantao Huang, Yupeng Su, Hong Huang, Ngai Wong, Hao Yu', 'author_detail': {'name': 'Ziyi Guan, Hantao Huang, Yupeng Su, Hong Huang, Ngai Wong, Hao Yu'}}
        """
        return {
            "type": "arxiv",
            "category": self.category,
            "id": post.id,
            "title": post.title,
            "summary": post.summary,
            "author": post.author,
            "url": post.link,
        }

    def fetch(self, n: int):
        try:
            feed = feedparser.parse(self.feed_url)
            entries = feed.entries[:n]
            parsed_entries = [self.parse(entry) for entry in entries]
            return parsed_entries
        except Exception as e:
            logger.error(f"Error fetching ArXiv feed: {e}")
            return []
