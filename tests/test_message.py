from typing import NamedTuple

import pytest
from pydantic import ValidationError

from eventsourcing_helpers.message import Message, message_factory
from eventsourcing_helpers.message.pydantic import PydanticMixin


class MessageTests:
    def setup_method(self):
        self.data = {"id": 1, "foo": "bar", "baz": None, "foobar": {"a": "b"}}
        fields = [(k, None) for k in self.data.keys()]
        self.namedtuple = NamedTuple("FooEvent", fields)
        message_cls = message_factory(self.namedtuple, is_new=True)
        self.message = message_cls(**self.data)

    def test_type(self):
        """
        Test that the message has the correct types.
        """
        assert isinstance(self.message, Message)
        assert type(self.message).__name__ == "FooEvent"

    def test_message_attr_redirect(self):
        """
        Test that attribute lookups are redirected to the namedtuple.
        """
        assert self.message.id == self.data["id"]
        assert self.message.foo == self.data["foo"]

    def test_access_missing_attr_on_new_msg_should_raise_attribute_error(self):
        with pytest.raises(AttributeError):
            self.message.boo

    def test_access_missing_attr_on_old_msg_should_return_none(self):
        message_cls = message_factory(self.namedtuple, is_new=False)
        message = message_cls(**self.data)
        assert message.boo is None

    def test_to_dict(self):
        """
        Test that the message is serialized correctly.
        """
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

    def test_read_only_nested_data_type_new_message(self):
        foobar = self.message.foobar
        foobar["a"] = "c"
        assert self.message.foobar["a"] == "b"

    def test_read_only_nested_data_type_old_message(self):
        message_cls = message_factory(self.namedtuple, is_new=False)
        self.message = message_cls(**self.data)

        foobar = self.message.foobar
        foobar["a"] = "c"
        assert self.message.foobar["a"] == "b"


class PydanticMixinTests:
    def setup_method(self):
        self.data = {"id": 1, "foo": "bar", "baz": None, "foobar": {"a": "b"}}

        class Foobar(PydanticMixin):
            a: str

        class FooEvent(PydanticMixin):
            id: int
            foo: str
            baz: str | None
            foobar: Foobar

        self.message = FooEvent(**self.data)

    def test_to_dict(self):
        assert self.message.to_dict() == self.data

    def test_name(self):
        assert self.message._class == self.message.__class__.__name__

    def test_read_only(self):
        with pytest.raises(ValidationError):
            self.message.id = 2
