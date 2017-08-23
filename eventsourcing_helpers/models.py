import re

from eventsourcing_helpers import logger

word_regexp = re.compile('[A-Z][^A-Z]*')


class BaseAggregate:

    def __init__(self):
        self._id = None
        self._version = 0
        self._uncommitted_events = []

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def apply_events(self, events):
        logger.info("Apply historical events from repository")
        for event in events:
            self.apply(event, is_new=False)

    def mark_events_as_commited(self):
        logger.info("Marking new events as committed")
        self._uncommitted_events = []

    def apply(self, event, is_new=True):
        event_name = event.__class__.__name__

        words = re.findall(word_regexp, event_name)
        words = map(str.lower, words)
        apply_method_name = 'apply_' + '_'.join(words)

        log = logger.bind(event_name=event_name)

        try:
            log.info("Applying event", is_new=is_new)
            apply_method = getattr(self, apply_method_name)
        except AttributeError:
            log.error("Missing event apply method",
                      method=apply_method_name)
        else:
            apply_method(event)
            if is_new:
                self._uncommitted_events.append(event)
