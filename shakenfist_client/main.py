# Copyright 2020 Michael Still

import click
import json
import logging
from shakenfist_utilities import logs
import sys

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
    if not ctx.obj:
        ctx.obj = {}
    ctx.obj['OUTPUT'] = output

    if verbose:
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
