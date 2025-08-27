from eventsourcing_helpers.compat import require_major_at_least

try:
    import pydantic as _pyd
    from pydantic import BaseModel, ConfigDict
except ModuleNotFoundError as e:
    raise ImportError(
        "PydanticMixin is an optional feature. Install pydantic>=2 to use it "
        "(e.g., `pip install 'pydantic>=2'` or enable the project's 'pydantic' extra)."
    ) from e

require_major_at_least("pydantic", module=_pyd, required_major=2, feature_name="PydanticMixin")


class PydanticMixin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @property
    def _class(self) -> str:
        return self.__class__.__name__

    def to_dict(self):
        return self.model_dump()
