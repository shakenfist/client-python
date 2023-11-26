import click
import copy
import os
from shakenfist_client import apiclient
import subprocess
import sys
import tempfile
import time
import yaml


CLUSTER_LIST = 'orchestrated_k3s_clusters'
METADATA_KEY = 'orchestrated_k3s_cluster_%s'
LATEST_RELEASE = 'v1.28.2+k3s1'


def _emit_debug(ctx, m):
    if ctx.obj['VERBOSE']:
        print(m)


@click.group(help='k3s orchestration commands')
def k3s():
    pass


def _create_instance(ctx, md):
    node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
    inst = ctx.obj['CLIENT'].create_instance(
        node_name, 2, 2048,
        [
            {
                'network_uuid': md['node_network'],
                'macaddress': None,
                'model': 'virtio',
                'float': True
            }
        ],
        [
            {
                'size': 50,
                'base': 'debian:11',
                'bus': None,
                'type': 'disk'
            }
        ],
        None, None,
        side_channels=['sf-agent'],
        namespace=md['namespace']
    )
    md['node_serial'] += 1
    return inst


def _await_boot(ctx, instances):
    waiting = copy.copy(instances)
    while waiting:
        print('Waiting for %d instances to boot' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            print('...instance %s has state %s and agent state %s'
                  % (inst['name'], inst['state'], inst['agent_state']))
            if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                ctx.obj['CLIENT'].instance_execute(instance_uuid, 'apt-get update')
                ctx.obj['CLIENT'].instance_execute(instance_uuid, 'apt-get dist-upgrade -y')
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)


def _await_idle(ctx, instances):
    waiting = copy.copy(instances)
    while waiting:
        print('Waiting for %d instances to be idle' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            agent_ops = ctx.obj['CLIENT'].get_instance_agentoperations(instance_uuid, all=True)

            incomplete = 0
            for aop in agent_ops:
                if aop['state'] != 'complete':
                    incomplete += 1
            print('...instance %s has %d incomplete agent operations'
                  % (inst['name'], incomplete))

            if incomplete == 0:
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)


def _await_fetch(ctx, aop):
    while aop['state'] != 'complete':
        time.sleep(1)
        aop = ctx.obj['CLIENT'].get_agent_operation(aop['uuid'])

    blob_uuid = aop['results']['0']['content_blob']
    data = b''
    for chunk in ctx.obj['CLIENT'].get_blob_data(blob_uuid):
        data += chunk
    return data.decode('utf-8')


def _reap_execute(ctx, aop):
    while aop['state'] != 'complete':
        time.sleep(1)
        aop = ctx.obj['CLIENT'].get_agent_operation(aop['uuid'])

    if aop['results']['0']['return-code'] != 0:
        print('Command failed!')
        print('  command: %s' % aop['commands'][0]['commandline'])
        print('exit code: %s' % aop['results']['0']['return-code'])
        print('   stdout: %s' % '\n   stdout: '.join(aop['results']['0']['stdout'].split('\n')))
        print('   stderr: %s' % '\n   stderr'.join(aop['results']['0']['stderr'].split('\n')))
        sys.exit(1)


@k3s.command(name='list', help='List managed k3s clusters')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_list(ctx, namespace=None, ):
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)
    if not namespace:
        namespace = ctx.obj['CLIENT'].namespace

    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])

    for cluster in all_clusters:
        print(cluster)


k3s.add_command(k3s_list)


@k3s.command(name='create', help='Create a new k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--worker-count', type=click.INT, help='The number of workers',
              default=2)
@click.option('--metal-address-count', type=click.INT,
              help=('The number of floating addresses to route into the virtual '
                    'network for metallb to manage'),
              default=5)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.option('--network', type=click.STRING,
              help=('Specify a network here to add these nodes to a pre-existing '
                    'network. Otherwise one will be created for this cluster.'))
@click.pass_context
def k3s_create(ctx, name=None, worker_count=None, metal_address_count=None,
               namespace=None, network=None):
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)
    if namespace:
        ns = ctx.obj['CLIENT'].get_namespace(namespace)
        if not ns:
            ctx.obj['CLIENT'].create_namespace(namespace)
            print('Created namespace %s' % namespace)
    else:
        namespace = ctx.obj['CLIENT'].namespace

    # Log general information
    md_key = METADATA_KEY % name
    _emit_debug(ctx, 'Using namespace %s and metadata key %s' % (namespace, md_key))

    # Ensure this name isn't already taken
    _emit_debug(ctx, 'Checking the cluster metadata key %s is free' % md_key)
    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])

    if name in all_clusters:
        print('Sorry, that cluster name is already taken')
        sys.exit(1)
    if md_key in namespace_md:
        print('Sorry, that cluster name is already taken')
        sys.exit(1)
    all_clusters.append(name)
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, CLUSTER_LIST, all_clusters)

    # A place to store outstanding agent operations
    aops = []

    # Create a network for nodes
    if network:
        node_network = ctx.obj['CLIENT'].get_network(network)
        if not node_network:
            print('Specified network does not exist')
            sys.exit(1)
    else:
        node_network = ctx.obj['CLIENT'].allocate_network(
            '10.0.0.0/16', True, True, 'k3s-%s-node' % name, namespace=namespace)
        print('Created %s as the node network (uuid %s)'
              % (node_network['name'], node_network['uuid']))
        while True:
            node_network = ctx.obj['CLIENT'].get_network(node_network['uuid'])
            if node_network['state'] == 'created':
                break
            time.sleep(1)
        print('...Node network ready')

    # Initialise the metadata
    _emit_debug(ctx, 'Initialize cluster metadata')
    md = {
        'name': name,
        'namespace': namespace,
        'type': 'k3s',
        'k3s_version': LATEST_RELEASE,
        'state': 'initial',
        'node_serial': 1,
        'node_network': node_network['uuid'],
        'node_token': None,
        'control_plane_nodes': [],
        'worker_nodes': []
    }
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # We really should do a pre-fetch on the disk image and wait for it to
    # download before starting instances. That way the point of slowness is
    # more obvious. That requires cluster operations to exist though.

    # Start a single control plane server
    inst = _create_instance(ctx, md)
    md['control_plane_nodes'].append(inst['uuid'])
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)
    print('Created %s as a control plane node (uuid %s)'
          % (inst['name'], inst['uuid']))

    # Create two worker nodes
    for i in range(worker_count):
        inst = _create_instance(ctx, md)
        md['worker_nodes'].append(inst['uuid'])
        ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)
        print('Created %s as a worker node (uuid %s)'
              % (inst['name'], inst['uuid']))

    # Wait for instances to boot
    _await_boot(ctx, md['control_plane_nodes'] + md['worker_nodes'])

    # Record the node network address for the first control plane node as the API
    # address
    interfaces = ctx.obj['CLIENT'].get_instance_interfaces(md['control_plane_nodes'][0])
    md['api_address_inner'] = interfaces[0]['ipv4']
    md['api_address_floating'] = interfaces[0]['floating']
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Write a configuration file with the external address to the first control
    # plane node. This is needed so that the SSL certificate includes this
    # external name.
    print('Configuring k3s')
    aops.append(ctx.obj['CLIENT'].instance_execute(
        md['control_plane_nodes'][0], 'mkdir -p /etc/rancher/k3s/'))
    aops.append(ctx.obj['CLIENT'].instance_execute(
        md['control_plane_nodes'][0],
        'cat - > /etc/rancher/k3s/config.yaml << EOF\n'
        'write-kubeconfig-mode: "0644"\n'
        'tls-san:\n'
        '  - "%s"\n'
        'cluster-init: true\n'
        'EOF\n'
        % md['api_address_floating']))

    # Instruct the first control plane node to install k3s and helm
    print('Installing k3s on the first control plane node')
    for cmd in [
        'curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=%s sh -' % LATEST_RELEASE,
        ('curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | '
         'sudo tee /usr/share/keyrings/helm.gpg'),
        'sudo apt-get install -y apt-transport-https',
        ('echo "deb [arch=$(dpkg --print-architecture) '
         'signed-by=/usr/share/keyrings/helm.gpg] '
         'https://baltocdn.com/helm/stable/debian/ all main" | '
         'sudo tee /etc/apt/sources.list.d/helm-stable-debian.list'),
        'sudo apt-get update',
        'sudo apt-get install -y helm'
    ]:
        aops.append(ctx.obj['CLIENT'].instance_execute(md['control_plane_nodes'][0], cmd))

    # Wait for instances to be idle and check results
    _await_idle(ctx, md['control_plane_nodes'] + md['worker_nodes'])
    for aop in aops:
        _reap_execute(ctx, aop)
    aops = []

    # Fetch kubecfg, correct IP, and include cluster name instead of "default"
    print('Fetching kubecfg for cluster')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/etc/rancher/k3s/k3s.yaml')
    kubeconfig = _await_fetch(ctx, aop).replace(
        '127.0.0.1', md['api_address_floating'])

    kc = yaml.safe_load(kubeconfig)
    fqcn = '%s.%s' % (name, namespace)
    kc['clusters'][0]['name'] = fqcn
    kc['contexts'][0]['name'] = fqcn
    kc['contexts'][0]['context']['cluster'] = fqcn
    kc['contexts'][0]['context']['user'] = fqcn
    kc['users'][0]['name'] = fqcn
    kc['current-context'] = fqcn
    md['kubeconfig'] = yaml.dump(kc)
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Fetch the node token from the first control plane node
    print(
        'Fetching node registration token from first control plane node')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
    md['node_token'] = _await_fetch(ctx, aop).rstrip()
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Add worker nodes to the cluster
    print('Adding worker nodes')
    for instance_uuid in md['worker_nodes']:
        aops.append(ctx.obj['CLIENT'].instance_execute(
            instance_uuid,
            'curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=%s '
            'K3S_URL=https://%s:6443 K3S_TOKEN=%s sh -'
            % (LATEST_RELEASE, md['api_address_inner'], md['node_token'])))

    # Wait for instances to be idle and check results
    _await_idle(ctx, md['control_plane_nodes'] + md['worker_nodes'])
    for aop in aops:
        _reap_execute(ctx, aop)
    aops = []

    # Collect some addresses to route into the kube cluster for metallb to
    # manage
    print('Setting up metalb')
    md['routed_addresses'] = []
    for i in range(metal_address_count):
        addr = ctx.obj['CLIENT'].route_network_address(node_network['uuid'])
        if addr:
            md['routed_addresses'].append(addr)
            print('Allocated routed address %s' % addr)
    print('Allocated %d routed addresses' % len(md['routed_addresses']))
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Setup metallb for traffic ingress, guided by
    # https://itnext.io/kubernetes-loadbalancer-service-for-on-premises-6b7f75187be8
    metal_lb_config = ('cat - > /etc/sf/metallb-range-allocation.yaml << EOF\n'
                       'apiVersion: metallb.io/v1beta1\n'
                       'kind: IPAddressPool\n'
                       'metadata:\n'
                       '  name: empty\n'
                       '  namespace: metallb-system\n'
                       'spec:\n'
                       '  addresses:\n'
                       '  - %s/32\n'
                       '---\n'
                       'apiVersion: metallb.io/v1beta1\n'
                       'kind: L2Advertisement\n'
                       'metadata:\n'
                       '  name: empty\n'
                       '  namespace: metallb-system\n'
                       'EOF\n'
                       % '/32\n  - '.join(md['routed_addresses']))

    for cmd in [
        'kubectl create ns metallb-system',
        ('KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm '
         'upgrade --install -n metallb-system metallb '
         'oci://registry-1.docker.io/bitnamicharts/metallb'),
        'sleep 5',
        ('kubectl wait --kubeconfig /etc/rancher/k3s/k3s.yaml -n metallb-system pod '
         '--for=condition=Ready -l app.kubernetes.io/name=metallb --timeout=60s'),
        'mkdir -p /etc/sf',
        metal_lb_config,
        'kubectl apply -f /etc/sf/metallb-range-allocation.yaml'
    ]:
        aops.append(ctx.obj['CLIENT'].instance_execute(
            md['control_plane_nodes'][0], cmd))

    # Wait for instances to be idle and check results
    _await_idle(ctx, md['control_plane_nodes'] + md['worker_nodes'])
    for aop in aops:
        _reap_execute(ctx, aop)
    aops = []

    # Fetch the kubeconfig and install it
    with tempfile.TemporaryDirectory() as tempdir:
        new_config_path = os.path.join(tempdir, 'config')
        kube_dir = os.path.join(os.path.expanduser('~'), '.kube')
        main_config_path = os.path.join(kube_dir, 'config')
        os.makedirs(kube_dir, exist_ok=True)

        with open(new_config_path, 'w') as f:
            f.write(yaml.dump(kc))
        p = subprocess.run(
            'kubectl config view --flatten', shell=True, capture_output=True,
            env={
                'KUBECONFIG': ('%s/.kube/config:%s'
                               % (os.path.expanduser('~'), new_config_path))
                })
        if p.returncode != 0:
            print('Failed up update %s/.kube/config, return code %d'
                  % (os.path.expanduser('~'), p.returncode))
            sys.exit(1)
        with open(main_config_path, 'wb') as f:
            f.write(p.stdout)


k3s.add_command(k3s_create)


@k3s.command(name='getconfig', help='Get kubeconfig for an existing k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_getconfig(ctx, name=None, namespace=None):
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)
    if not namespace:
        namespace = ctx.obj['CLIENT'].namespace

    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    md_key = METADATA_KEY % name
    if md_key not in namespace_md:
        print('Unknown cluster')
        sys.exit(1)

    kubeconfig = namespace_md[md_key].get('kubeconfig')
    if not kubeconfig:
        print('No kubeconfig for this cluster. Is it fully installed?')
        sys.exit(1)

    print(kubeconfig)


@k3s.command(name='delete', help='Destroy a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_delete(ctx, name=None, namespace=None):
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)

    # Log general information
    md_key = METADATA_KEY % name
    _emit_debug(ctx, 'Using namespace %s and metadata key %s' % (namespace, md_key))

    # Ensure this name exists
    _emit_debug(ctx, 'Checking the cluster metadata key %s is present' % md_key)
    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    if md_key not in namespace_md:
        print('Sorry, that cluster name does not appear to exist')
        sys.exit(1)

    md = namespace_md[md_key]
    _emit_debug(ctx, 'Cluster metadata:')
    for k in md:
        _emit_debug(ctx, '    %s = %s' % (k, md[k]))

    # Delete instances
    waiting = []
    for instance_uuid in set(md['control_plane_nodes'] + md['worker_nodes']):
        try:
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            _emit_debug(ctx, '...Deleting instance %s with uuid %s'
                        % (inst['name'], instance_uuid))
            ctx.obj['CLIENT'].delete_instance(instance_uuid)
            waiting.append(instance_uuid)
        except apiclient.ResourceNotFoundException:
            pass

    while waiting:
        _emit_debug(ctx, '...Waiting for %d instances to be deleted' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            try:
                i = ctx.obj['CLIENT'].get_instance(instance_uuid)
                if i['state'] == 'deleted':
                    waiting.remove(instance_uuid)
            except apiclient.ResourceNotFoundException:
                waiting.remove(instance_uuid)

        if waiting:
            time.sleep(1)

    md['control_plane_nodes'] = []
    md['worker_nodes'] = []
    md['api_floating_address'] = None
    md['api_inner_address'] = None
    md['k3s_version'] = None
    md['kubeconfig'] = None
    md['node_token'] = None
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Free any routed ips
    for addr in md.get('routed_addresses', []):
        try:
            ctx.obj['CLIENT'].unroute_network_address(md['node_network'], addr)
        except apiclient.UnauthorizedException:
            _emit_debug(ctx, '...Address %s was not routed to this network' % addr)

    # Delete node network
    if md['node_network']:
        ctx.obj['CLIENT'].delete_network(md['node_network'])
        md['node_network'] = []

    md['state'] = 'deleted'
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)

    # Then remove the metadata
    ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, md_key)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])
    all_clusters.remove(name)
    if not all_clusters:
        ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, CLUSTER_LIST)
    else:
        ctx.obj['CLIENT'].set_namespace_metadata_item(
            namespace, CLUSTER_LIST, all_clusters)

    # And remove the local config
    fqcn = '%s.%s' % (name, namespace)
    for config_elem in ['users.%s' % fqcn,
                        'contexts.%s' % fqcn,
                        'clusters.%s' % fqcn]:
        p = subprocess.run(
            'kubectl config unset %s' % config_elem, shell=True)
        if p.returncode != 0:
            print('Could not unset kubectl config element %s' % config_elem)
            sys.exit(1)


k3s.add_command(k3s_delete)
