from typing import NamedTuple

import pytest

from eventsourcing_helpers.message import message_factory, Message


class MessageTests:

    def setup_method(self):
        self.data = {'id': 1, 'foo': 'bar', 'baz': None}
        fields = [(k, None) for k in self.data.keys()]
        self.namedtuple = NamedTuple('FooEvent', fields)
        self.message = message_factory(self.namedtuple)(**self.data)

    def test_type(self):
        """
        Test that the message has the correct types.
        """
        assert isinstance(self.message, Message)
        assert type(self.message).__name__ == 'FooEvent'

    def test_message_attr_redirect(self):
        """
        Test that attribute lookups are redirected to the namedtuple.
        """
        assert self.message.id == self.data['id']
        assert self.message.foo == self.data['foo']

    def test_to_dict(self):
        """
        Test that the message is serialized correctly.
        """
        self.data.pop('baz')
        assert self.message.to_dict() == self.data

    def test_name(self):
        """
        Test that the correct name is returned.
        """
        assert self.message._class == self.namedtuple.__name__

    def test_read_only(self):
        """
        Test that we can't mutate the state of a message.
        """
        with pytest.raises(AttributeError):
            self.message.id = 2
