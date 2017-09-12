import re
import uuid

from eventsourcing_helpers import logger

word_regexp = re.compile('[A-Z][^A-Z]*')


class Entity:

    _events = []

    def __init__(self):
        self._guid = None
        self._version = 0

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def _create_guid(self):
        return str(uuid.uuid4())

    def apply_events(self, events):
        logger.info("Apply historical events from repository")
        for event in events:
            self.apply(event, is_new=False)

    def clear_commited_events(self):
        logger.info("Clearing commited events")
        Entity._events = []

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
                self._events.append(event)


class AggregateRoot(Entity):
    pass
