import datetime
import json
import functools

from pyramid.view import view_config
from pyramid.renderers import get_renderer

from .. import exception
from ..util import (
    datestamp_now,
    format_datestamp,
    parse_date,
    contains_illegal_chars,
)

from ..models import (
    Item,
    Record,
    Format,
    Datestamp,
    Set,
)


def oai_view(wrapped):
    """Augment the return value of a function with common template
    parameters and add time property to the request parameter."""

    def wrapper(context, request=None):
        if request is None:
            request = context
            context = None

        # Get the datestamp before any database queries.
        setattr(request, 'time', datestamp_now())

        if wrapped.__code__.co_argcount == 1:
            result = wrapped(request)
        else:
            result = wrapped(context, request)

        # time of the response
        result['time'] = request.time
        # function for formatting datestamps
        result['format_date'] = format_datestamp

        request.response.content_type = 'text/xml'
        return result

    functools.update_wrapper(wrapper, wrapped)
    return wrapper


@view_config(context=exception.OaiException,
             renderer='templates/error.pt')
@oai_view
def oai_error_view(error, request):
    # Called when some other view raises an OaiException.
    return {'error': error}


@view_config(route_name='oai')
@oai_view
def invalid_verb_view(request):
    # Called when the verb argument does not match any other view.
    if 'verb' in request.params:
        raise exception.InvalidVerb()
    raise exception.MissingVerb()


@view_config(route_name='oai',
             request_param='verb=Identify',
             renderer='templates/identify.pt')
@oai_view
def handle_identify(request):
    _check_params(request.params)

    ignore_deleted = _get_ignore_deleted(request)
    earliest = Record.earliest_datestamp(ignore_deleted)

    # Current time is a lower bound when there are no records.
    context = {'earliest': earliest or request.time}
    for value in ['repository_name',
                  'admin_emails',
                  'deleted_records',
                  'repository_descriptions',
                 ]:
        context[value] = request.registry.settings[value]
    return context


@view_config(route_name='oai',
             request_param='verb=ListSets',
             renderer='templates/listsets.pt')
@oai_view
def handle_list_sets(request):
    try:
        if _get_resumption_token(request):
            # Resumption tokens are not used for ListSets.
            raise exception.InvalidResumptionToken()
    except exception.ExpiredResumptionToken:
        raise exception.InvalidResumptionToken()

    _check_params(request.params)

    sets = Set.list()
    if len(sets) == 0:
        raise exception.NoSetHierarchy()
    else:
        return {'sets': sets}


@view_config(route_name='oai',
             request_param='verb=ListMetadataFormats',
             renderer='templates/listformats.pt')
@oai_view
def handle_list_metadata_formats(request):
    _check_params(request.params, allowed=['identifier'])

    ignore_deleted = _get_ignore_deleted(request)
    identifier = _get_identifier(request.params, ignore_deleted)
    formats = Format.list(identifier, ignore_deleted)

    if identifier is not None and not formats:
        raise exception.NoMetadataFormats(identifier)

    # At least Dublin Core should be supported so formats should not be
    # empty.
    assert formats, 'No metadata formats supported'

    return {'formats': formats}


@view_config(route_name='oai',
             request_param='verb=ListIdentifiers',
             renderer='templates/listidentifiers.pt')
@view_config(route_name='oai',
             request_param='verb=ListRecords',
             renderer='templates/listrecords.pt')
@oai_view
def handle_list_items(request):
    limit = request.registry.settings['item_list_limit']

    token_params = _get_resumption_token(request)
    has_token = (token_params is not None)
    params = token_params or request.params

    if has_token:
        required = ['metadataPrefix',
                    'offset',
                    'date',
                    'from',
                    'until',
                    'set']
        allowed = []
    else:
      required = ['metadataPrefix']
      allowed = ['from', 'until', 'set']

    try:
        _check_params(params, required=required, allowed=allowed)
        ignore_deleted = _get_ignore_deleted(request)
        records, next_offset = _get_records(params, ignore_deleted, limit)
    except exception.OaiException:
        if has_token:
            # Raise a BadResumptionToken instead since the parameters were
            # parsed from the token.
            raise exception.InvalidResumptionToken()
        raise

    if next_offset is not None:
        # Need to send a resumption token.
        new_token = _create_resumption_token(
            params, next_offset, request.time)
    elif token_params is not None:
        # Send an empty resumption token with the last set of results.
        new_token = ''
    else:
        # No resumption token needed.
        new_token = None

    return {'records': records, 'token': new_token}


def _create_resumption_token(params, offset, time):
    """Create a resumption token for a ListRecords or ListIdentifiers
    request.
    """
    return json.dumps({
        'verb': params['verb'],
        'metadataPrefix': params['metadataPrefix'],
        'offset': offset,
        'date': format_datestamp(time),
        'from': params.get('from', None),
        'until': params.get('until', None),
        'set': params.get('set', None),
    })


@view_config(route_name='oai',
             request_param='verb=GetRecord',
             renderer='templates/getrecord.pt')
@oai_view
def handle_get_record(request):
    _check_params(request.params,
                  required=['identifier', 'metadataPrefix'])

    ignore_deleted = _get_ignore_deleted(request)
    identifier = _get_identifier(request.params, ignore_deleted)
    prefix = _get_metadata_prefix(request.params, ignore_deleted)

    records = Record.list(
        identifier=identifier,
        metadata_prefix=prefix,
        ignore_deleted=ignore_deleted,
    )

    if not records:
        raise exception.UnavailableMetadataFormat(prefix, identifier)
    assert len(records) == 1, 'Id-prefix combination is not unique'

    return {'record': records[0]}


def _check_params(params, required=[], allowed=[]):
    """Check that request parameters are valid.

    The required ``verb`` parameters is implied.

    Parameters
    ----------
    params: dict or multidict
        The arguments to be checked.
    required: list of str
        The required parameter names.
    allowed: list of str
        The optional parameter names.

    Raises
    ------
    MissingVerb:
        If the ``verb`` parameter is missing.
    RepeatedVerb:
        If the ``verb`` parameter is repeated.
    BadArgument:
        If some arguments are missing, illegal or repeated.
    """
    # Check verb.
    if 'verb' not in params:
        raise exception.MissingVerb()
    if hasattr(params, 'getall') and len(params.getall('verb')) > 1:
        raise exception.RepeatedVerb()

    for name in params:
        is_expected = (
            (name == 'verb') or
            (name in allowed) or
            (name in required)
        )
        if not is_expected:
            raise exception.BadArgument('Illegal argument: "%s"' % name)

        is_repeated = (
            hasattr(params, 'getall') and
            len(params.getall(name)) > 1
        )
        if is_repeated:
            raise exception.BadArgument('Repeated argument: "%s"' % name)

        value = params.get(name)
        if value is not None and contains_illegal_chars(value):
            raise exception.BadArgument('Invalid argument: "%s"' % name)

    # Check required arguments.
    for p in required:
        if p not in params:
            raise exception.BadArgument('Missing argument: "%s"' % p)


def _get_resumption_token(request):
    """Check whether the request parameters contain a resumption token.

    Also check that the resumption token is not expired and that it has the
    correct verb. Other parameters in the resumption token are not checked.

    Parameters
    ----------
    request: pyramid.request.Request
        The request.

    Raises
    ------
    BadArgument:
        If the parameters contain other arguments in addition to the
        resumption token, or if the params contain multiple resumption
        tokens.
    InvalidResumptionToken:
        If the params contain an invalid resumption token.
    ExpiredResumptionToken:
        If the resumption token has expired.

    Return
    ------
    None or dict:
        The parsed resumption token dict, or ``None`` if there is no
        request token in the parameters.
    """
    if 'resumptionToken' not in request.params:
        return None
    # No other arguments allowed with resumptionToken.
    _check_params(request.params, required=['resumptionToken'])
    try:
        parsed = json.loads(request.params['resumptionToken'])
    except:
        raise exception.InvalidResumptionToken()

    # Check types.
    if type(parsed) is not dict:
        raise exception.InvalidResumptionToken()
    for k, v in parsed.items():
        if (v is not None) and (not isinstance(v, str)):
            raise exception.InvalidResumptionToken()

    # Check verb.
    if parsed.get('verb', None) != request.params['verb']:
        raise exception.InvalidResumptionToken()

    # Check date.
    _check_resumption_token_date(parsed)

    return parsed


def _check_resumption_token_date(token):
    """Check that a resumption token's date is valid.

    Arguments
    ---------
    token: dict from str to str
        The parsed resumption token.

    Raises
    ------
    InvalidResumptionToken:
        If the date is in invalid format.
    ExpiredResumptionToken:
        If the resumption token has expired.
    """
    try:
        date, _ = parse_date(token['date'])
    except:
        raise exception.InvalidResumptionToken()
    latest = Datestamp.get()
    if (latest is not None) and (latest >= date):
        raise exception.ExpiredResumptionToken()


def _get_ignore_deleted(request):
    return request.registry.settings['deleted_records'] == 'no'


def _get_metadata_prefix(params, ignore_deleted):
    """Check that metadata prefix in request parameters is supported.

    If the metadata prefix is not supported, raise
    ``UnsupportedMetadataFormat``. Otherwise return the prefix.
    """
    prefix = params['metadataPrefix']
    if not Format.exists(prefix, ignore_deleted):
        raise exception.UnsupportedMetadataFormat(prefix)
    return prefix


def _get_identifier(params, ignore_deleted):
    """Get identifier from request parameters and check that it exists.

    Parameters
    ----------
    params: dict
        The request parameters.
    ignore_deleted: bool
        If `True` consider deleted items as not existing.

    Raises
    ------

    Return
    ------
    If there is no identifier in ``params``, return ``None``. If params
    contains an invalid identifier, raise ``IdDoesNotExist``. Otherwise
    return the identifier.
    """
    if 'identifier' not in params:
        return None
    identifier = params['identifier']
    if not Item.exists(identifier, ignore_deleted):
        raise exception.IdDoesNotExist(identifier)
    return identifier


def _get_records(params, ignore_deleted, limit):
    """Fetch records from the model.

    Parameters
    ----------
    params: multidict
        The request parameters.
    ignore_deleted: bool
        If `True`, filter out deleted records.
    limit: int
        Maximum number of records to fetch.

    Return
    ------
    list of object:
        The fetched records.
    str or None:
        The identifier of the next record, if there are more records left.
        Otherwise ``None``.

    Raises
    ------
    BadArgument:
        If some parameter is invalid.
    NoSetHierarchy:
        If sets are not supported.
    NoRecordsMatch:
        If there are no matching records.
    UnsupportedMetadataFormat:
        If the ``metadataPrefix`` parameter is not supported.
    """
    prefix = _get_metadata_prefix(params, ignore_deleted)

    from_date, until_date = _parse_from_and_until(
        params.get('from'), params.get('until'),
    )

    if params.get('set') is not None and len(Set.list()) == 0:
        raise exception.NoSetHierarchy()

    records = Record.list(
        metadata_prefix=prefix,
        from_date=from_date,
        until_date=until_date,
        set_=params.get('set'),
        ignore_deleted=ignore_deleted,
        offset=params.get('offset'),

        # Try to fetch one extra record to see wheter there are records
        # left, i.e. wheter we need to send a resumption token.
        limit=limit + 1,
    )

    if not records:
        raise exception.NoRecordsMatch()

    if len(records) == limit + 1:
        # More records left.
        return records[:-1], records[-1].identifier
    # Got all records.
    return records, None


def _parse_from_and_until(from_date_str, until_date_str):
    """Parse from and until argument strings.

    Parameters
    ----------
    from_date_str: str
        The ``from`` request parameter or ``None``.
    until_date_str: str
        The ``until`` request parameter or ``None``.

    Raises
    ------
    BadArgument:
        If either argument is in invalid format or they have different
        granularities or ``from`` is greater than ``until``.

    Return
    ------
    from_date: datetime.datetime or None
        The parsed ``from`` parameter, or ``None`` if it was not given.
    until_date: datetime.datetime or None
        The parsed ``until`` parameter, or ``None`` if it was not given.
    """
    from_date = None
    from_granularity = None
    if from_date_str is not None:
        try:
            from_date, from_granularity = parse_date(from_date_str)
        except ValueError:
            raise exception.BadArgument('Illegal "from" datestamp')

    until_date = None
    until_granularity = None
    if until_date_str is not None:
        try:
            until_date, until_granularity = parse_date(
                until_date_str,
                datetime.time(23, 59, 59),
            )
        except ValueError:
            raise exception.BadArgument('Illegal "until" datestamp')

    if (from_date is not None and until_date is not None):
        if (from_granularity != until_granularity):
            raise exception.BadArgument(
                'Datestamps "from" and "until" have different granularity'
            )
        if (from_date > until_date):
            raise exception.BadArgument(
                'Datestamp "from" is greater than "until"'
            )
    return from_date, until_date
