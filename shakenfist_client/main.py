# Copyright 2020 Michael Still
import json
import logging
import sys

import click
try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points
from shakenfist_utilities import logs

from shakenfist_client import apiclient
from shakenfist_client.commandline import admin
from shakenfist_client.commandline import artifact
from shakenfist_client.commandline import backup
from shakenfist_client.commandline import blob
from shakenfist_client.commandline import instance
from shakenfist_client.commandline import interface
from shakenfist_client.commandline import label
from shakenfist_client.commandline import namespace
from shakenfist_client.commandline import network
from shakenfist_client.commandline import node


LOG = logs.setup_console(__name__)
CLIENT = None


class RedactTokensFilter(logging.Filter):
    """Replace JWTs in log records with a placeholder.

    A Kerbside VDI console URL carries its own capability as a
    short-lived, single-use JWT, and two separate debug paths print that
    URL without knowing it is a credential: apiclient's _request_url()
    logs the body of the /vdiconsoleproxy response, and urllib3 logs the
    request target of every connection it makes. Redacting on the way
    out of the logging system covers both, in a way that patching each
    call site does not.

    This is the same choice _request_url() already makes by hand for the
    Authorization header, generalised. It applies to sf-client's own
    output only; a library caller configures their own handlers and
    makes their own decision.

    It covers logging, and only logging. A credential can leave by
    other routes -- an exception message rendered as a traceback is the
    one that bit us -- so apiclient redacts those where it raises them
    rather than relying on this.
    """

    def filter(self, record):
        message = record.getMessage()
        redacted = apiclient.redact_tokens(message)
        if redacted != message:
            # args are already interpolated into the redacted message, so
            # they must be dropped or getMessage() would try again.
            record.msg = redacted
            record.args = ()
        return True


# One instance, because addFilter() deduplicates by identity and
# configure_logging() may run more than once in a test process.
REDACT_TOKENS = RedactTokensFilter()


def configure_logging():
    """Give the root logger a handler, and redact tokens on the way out.

    setup_console() raises the root logger's level to INFO, but attaches
    its handler to this module's logger only. Records from every other
    module -- ours and our dependencies' alike -- therefore propagate up
    to a root logger with no handler on it and are dropped, so sf-client
    would print its own INFO lines and nothing else. basicConfig() gives
    root a handler. Once root has one, our own records reach both it and
    the handler setup_console() installed and are printed twice, which is
    what turning off propagation prevents.

    Root keeps basicConfig's stderr rather than the stdout that
    ConsoleLoggingHandler print()s to, so that a urllib3 warning cannot
    corrupt the output of `sf-client --json`. The format is matched by
    hand so the two streams do not read as two different programs; the
    logger name is kept on this one because a record from a dependency
    is only useful once you know which dependency emitted it.

    Called from cli() rather than run at import: this reconfigures
    logging for the whole process, which is sf-client's business when it
    is the program being run and nobody else's when a plugin, a test or
    an embedding program merely imports this module.

    The redaction filter reaches the handlers that exist when this runs,
    which for a console script is all of them. A handler attached later
    -- by a plugin, or by a library configuring itself lazily -- is not
    covered, which is why apiclient redacts the tokens it raises in
    exceptions itself rather than leaving it to logging.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(name)s: %(message)s')
    # basicConfig() puts its setLevel() inside "if root has no handlers",
    # so if anything imported before us already gave root one, the level
    # above is silently discarded. Set it ourselves; by the time cli()
    # runs, sf-client is the program and the root level is its call.
    logging.root.setLevel(logging.INFO)
    logging.getLogger(__name__).propagate = False

    for handler in (logging.root.handlers +
                    logging.getLogger(__name__).handlers):
        handler.addFilter(REDACT_TOKENS)


class GroupCatchExceptions(click.Group):
    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)

        except apiclient.RequestMalformedException as e:
            LOG.error('Malformed Request: %s' % error_text(e.text))
            sys.exit(1)

        except apiclient.UnauthenticatedException as e:
            LOG.error('Not authenticated: %s' % e)
            sys.exit(1)

        except apiclient.UnauthorizedException as e:
            LOG.error('Not authorized: %s' % error_text(e.text))
            sys.exit(1)

        except apiclient.ResourceNotFoundException as e:
            LOG.error('Resource not found: %s' % error_text(e.text))
            sys.exit(1)

        except apiclient.DependenciesNotReadyException as e:
            LOG.error('Dependencies not ready: %s' % error_text(e.text))
            sys.exit(1)

        except apiclient.ResourceStateConflictException as e:
            LOG.error('Resource state conflict: %s' % error_text(e.text))
            sys.exit(1)

        except apiclient.InternalServerError as e:
            # Print full error since server should not fail
            LOG.error('Internal Server Error: %s' % e.text)
            sys.exit(1)

        except apiclient.InsufficientResourcesException as e:
            LOG.error('Insufficient Resources: %s' %
                      error_text(e.text))
            sys.exit(1)

        except apiclient.ServiceUnavailableException as e:
            # A retryable refusal, not a durable one. The namespace claims
            # API answers this while the cluster capacity accounting is
            # still being built, and when a claim was contended for longer
            # than the optimistic retry budget allowed -- so the message
            # has to tell an operator the request was fine and to try it
            # again, rather than to go looking for what they got wrong.
            LOG.error('Service unavailable, please retry: %s' %
                      error_text(e.text))
            sys.exit(1)

        except apiclient.AgentOperationFailed as e:
            # The three agent verbs (instance execute, upload and download)
            # call the creating helpers directly, and those now raise as
            # soon as the operation reaches a terminal failure state rather
            # than handing back an in flight operation for the caller to
            # give up on. Once the server side deadlines deploy, "expired"
            # makes that a routine outcome rather than an edge case, so it
            # has to read as an error message and exit 1 like every other
            # failure here, not as a traceback.
            LOG.error('Agent operation failed: %s' % e)
            sys.exit(1)

        except apiclient.requests.exceptions.ConnectionError as e:
            LOG.error('Unable to connect to server: %s' % e)
            sys.exit(1)


def error_text(json_text):
    try:
        err = json.loads(json_text)
        if 'error' in err:
            return err['error']
    except Exception:
        pass

    return json_text


@click.group(cls=GroupCatchExceptions)
@click.option('--pretty', 'output', flag_value='pretty', default=True)
@click.option('--simple', 'output', flag_value='simple')
@click.option('--json', 'output', flag_value='json')
@click.option('--verbose/--no-verbose', default=False)
@click.option('--namespace', envvar='SHAKENFIST_NAMESPACE', default=None)
@click.option('--key', envvar='SHAKENFIST_KEY', default=None)
@click.option('--apiurl', envvar='SHAKENFIST_API_URL', default=None)
@click.option('--async-strategy', '--async', envvar='SHAKENFIST_ASYNC', default='pause',
              type=click.Choice(['continue', 'pause', 'block'], case_sensitive=False))
@click.pass_context
def cli(ctx, output, verbose, namespace, key, apiurl, async_strategy):
    configure_logging()

    if not ctx.obj:
        ctx.obj = {}
    ctx.obj['OUTPUT'] = output
    ctx.obj['VERBOSE'] = verbose

    if verbose:
        # Root and its handlers as well as LOG. LOG's own records go
        # straight to the handler setup_console() installed, so raising
        # its level alone would make sf-client's lines verbose and leave
        # every other module -- including requests and urllib3, where the
        # answer to "why did that call fail" usually is -- filtered at
        # INFO by a root logger nobody moved. urllib3 logs the full
        # request target of every connection, which is why
        # configure_logging() installs RedactTokensFilter before we get
        # here rather than trusting each library to know a credential
        # when it sees one.
        logging.root.setLevel(logging.DEBUG)
        for handler in logging.root.handlers:
            handler.setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)
        LOG.debug('Set log level to DEBUG')
    else:
        LOG.setLevel(logging.INFO)

    global CLIENT
    CLIENT = apiclient.Client(
        namespace=namespace,
        key=key,
        base_url=apiurl,
        logger=LOG,
        async_strategy=async_strategy)
    ctx.obj['CLIENT'] = CLIENT
    LOG.debug('Client for %s constructed' % apiurl)


@cli.command(name='version', help='Output the version of the client')
@click.pass_context
def version(ctx):
    print(apiclient.get_user_agent())


cli.add_command(admin.admin)
cli.add_command(artifact.artifact)
cli.add_command(backup.backup)
cli.add_command(blob.blob)
cli.add_command(instance.instance)
cli.add_command(interface.interface)
cli.add_command(label.label)
cli.add_command(namespace.namespace)
cli.add_command(network.network)
cli.add_command(node.node)
cli.add_command(version)


# Load plugins
eps = entry_points()
# Python 3.10+
if hasattr(eps, 'select'):
    plugin_eps = eps.select(group='shakenfist_client.plugin')
# Python 3.9 and earlier
else:
    plugin_eps = eps.get('shakenfist_client.plugin', [])

for ep in plugin_eps:
    ep.load()(cli)
