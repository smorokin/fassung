class TransactionClosedError(Exception):
    """Raised when an operation is attempted on a transaction that is no longer active."""


class UnsupportedTemplateError(Exception):
    """Raised when a plain string is passed where a Template is required."""
