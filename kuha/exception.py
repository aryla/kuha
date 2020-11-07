from .util import filter_illegal_chars

class ConfigurationError(Exception):
    """Invalid settings in configuration file."""


class HarvestError(Exception):
    """Error while updating formats, items, sets or records."""


class OaiException(Exception):
    """Base class for exceptions representing an OAI-PMH error."""

    def code(self):
        """
        Return
        ------
        unicode:
            The OAI-PMH error code.
        """
        raise NotImplementedError()

    def message(self):
        """
        Return
        ------
        unicode:
            Return a human-readable error message.
        """
        raise NotImplementedError()


class BadArgument(OaiException):
    """Illegal or missing arguments"""

    def __init__(self, message):
        self._message = filter_illegal_chars(message)

    def code(self):
        return 'badArgument'

    def message(self):
        return self._message


class BadResumptionToken(OaiException):
    """Invalid or expired resumptionToken"""

    def code(self):
        return 'badResumptionToken'


class InvalidResumptionToken(BadResumptionToken):
    def message(self):
        return 'Invalid resumption token'


class ExpiredResumptionToken(BadResumptionToken):
    def message(self):
        return 'Resumption token has expired.'


class BadVerb(OaiException):
    """Invalid or missing verb"""

    def code(self):
        return 'badVerb'


class InvalidVerb(BadVerb):
    def message(self):
        return 'Invalid verb'


class RepeatedVerb(BadVerb):
    def message(self):
        return 'Repeated verb'


class MissingVerb(BadVerb):
    def message(self):
        return 'Missing verb'


class CannotDisseminateFormat(OaiException):
    """Given metadataPrefix is not supported"""

    def code(self):
        return 'cannotDisseminateFormat'


class UnsupportedMetadataFormat(CannotDisseminateFormat):
    def __init__(self, prefix):
        self._message = filter_illegal_chars(
            'Metadata format "{0}" is not supported by this repository.'
            ''.format(prefix)
        )

    def message(self):
        return self._message


class UnavailableMetadataFormat(CannotDisseminateFormat):
    def __init__(self, prefix, identifier):
        self._message = filter_illegal_chars(
            'Metadata format "{0}" is not available for item "{1}".'
            ''.format(prefix, identifier)
        )

    def message(self):
        return self._message


class IdDoesNotExist(OaiException):
    """Illegal of unknown item identifier"""

    def __init__(self, identifier):
        self._message = filter_illegal_chars(
            'Identifier "{0}" does not exist.'
            ''.format(identifier)
        )

    def code(self):
        return 'idDoesNotExist'

    def message(self):
        return self._message


class NoRecordsMatch(OaiException):
    """No matching records were found"""

    def code(self):
        return 'noRecordsMatch'

    def message(self):
        return 'No matching records found.'


class NoMetadataFormats(OaiException):
    """There are no supported metadata formats for an item"""

    def __init__(self, identifier):
        self._message = filter_illegal_chars(
            'No metadata formats available for item "%s".'
            ''.format(identifier)
        )

    def code(self):
        return 'noMetadataFormats'

    def message(self):
        return self._message


class NoSetHierarchy(OaiException):
    """The repository does not support sets"""

    def code(self):
        return 'noSetHierarchy'

    def message(self):
        return 'This repository does not support sets.'
