import requests
import subprocess
import time
import unittest
import collections


### initialize constants

hostname = 'localhost' # Windows and Mac users can change this to the docker vm ip
hostBaseUrl = 'http://{}'.format(hostname)

imageName = "asg3img"
subnetName = "asg3net"
subnetRange = "10.10.0.0/16"
containerPort = "8090"

class ReplicaConfig(collections.namedtuple('ReplicaConfig', ['name', 'addr', 'host_port'])):
    @property
    def socketAddress(self):
        return '{}:{}'.format(self.addr, containerPort)
    def __str__(self):
        return self.name

alice = ReplicaConfig(name='alice', addr='10.10.0.2', host_port=8082)
bob   = ReplicaConfig(name='bob',   addr='10.10.0.3', host_port=8083)
carol = ReplicaConfig(name='carol', addr='10.10.0.4', host_port=8084)
all_replicas = [alice, bob, carol]
viewStr = lambda replicas: ','.join(r.socketAddress for r in replicas)
viewSet = lambda replicas: set(r.socketAddress for r in replicas)

def sleep(n):
    multiplier = 1
    # Increase the multiplier if you need to during debugging, but make sure to
    # set it back to 1 and test your work before submitting.
    print('(sleeping {} seconds)'.format(n*multiplier))
    time.sleep(n*multiplier)


### docker linux commands

def removeSubnet(required=True):
    command = ['docker', 'network', 'rm', subnetName]
    print('removeSubnet:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def createSubnet():
    command = ['docker', 'network', 'create',
            '--subnet={}'.format(subnetRange), subnetName]
    print('createSubnet:', ' '.join(command))
    subprocess.check_call(command, stdout=subprocess.DEVNULL)

def buildDockerImage():
    command = ['docker', 'build', '-t', imageName, '.']
    print('buildDockerImage:', ' '.join(command))
    subprocess.check_call(command)

def runReplica(instance, view_replicas):
    assert view_replicas, 'the view can\'t be empty because it must at least contain this replica'
    command = ['docker', 'run', '--rm', '--detach',
        '--publish={}:{}'.format(instance.host_port, containerPort),
        "--net={}".format(subnetName),
        "--ip={}".format(instance.addr),
        "--name={}".format(instance.name),
        "-e=SOCKET_ADDRESS={}:{}".format(instance.addr, containerPort),
        "-e=VIEW={}".format(viewStr(view_replicas)),
        imageName]
    print('runReplica:', ' '.join(command))
    subprocess.check_call(command)

def stopAndRemoveInstance(instance, required=True):
    command = ['docker', 'stop', instance.name]
    print('stopAndRemoveInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)
    command = ['docker', 'remove', instance.name]
    print('stopAndRemoveInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def killInstance(instance, required=True):
    '''Kill is sufficient when containers are run with `--rm`'''
    command = ['docker', 'kill', instance.name]
    print('killInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def connectToNetwork(instance):
    command = ['docker', 'network', 'connect', subnetName, instance.name]
    print('connectToNetwork:', ' '.join(command))
    subprocess.check_call(command)

def disconnectFromNetwork(instance):
    command = ['docker', 'network', 'disconnect', subnetName, instance.name]
    print('disconnectFromNetwork:', ' '.join(command))
    subprocess.check_call(command)


### test suite

class TestHW3(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('= Cleaning up resources possibly left over from a previous run..')
        stopAndRemoveInstance(alice, required=False)
        stopAndRemoveInstance(bob,   required=False)
        stopAndRemoveInstance(carol, required=False)
        removeSubnet(required=False)
        sleep(1)
        print("= Creating resources required for this run..")
        createSubnet()

    def setUp(self):
        print("== Running replicas..")
        runReplica(alice, all_replicas)
        runReplica(bob,   all_replicas)
        runReplica(carol, all_replicas)
        sleep(3)

    def tearDown(self):
        print("== Destroying replicas..")
        killInstance(alice)
        killInstance(bob)
        killInstance(carol)

    @classmethod
    def tearDownClass(cls):
        print("= Cleaning up resources from this run..")
        removeSubnet()


    def test_value_operations(self):
        '''Does your store implement the basic key:value storage API?'''
        metadata = None

        print('>>> Put moon:cake into the store')

        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'moon'),
                json={'value':'cake', 'causal-metadata': metadata})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'created')
        metadata = response.json()['causal-metadata']

        print('... Wait for replication')
        sleep(5)

        print('=== Check moon:cake at replicas {}'.format(','.join(r.name for r in all_replicas)))
        for replica in all_replicas:
            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, replica.host_port, 'moon'),
                    json={'causal-metadata':metadata})
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('result', response.json(), msg='at replica, {}'.format(replica))
            self.assertIn('causal-metadata', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(response.json()['value'], 'cake', msg='at replica, {}'.format(replica))
            metadata = response.json()['causal-metadata']


        print('>>> Put moon:pie into the store')

        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'moon'),
                json={'value':'pie', 'causal-metadata':metadata})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'replaced')
        metadata = response.json()['causal-metadata']

        print('... Wait for replication')
        sleep(5)

        print('=== Check moon:pie at replicas {}'.format(','.join(r.name for r in all_replicas)))
        for replica in all_replicas:
            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, replica.host_port, 'moon'),
                    json={'causal-metadata':metadata})
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('result', response.json(), msg='at replica, {}'.format(replica))
            self.assertIn('causal-metadata', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(response.json()['value'], 'pie', msg='at replica, {}'.format(replica))
            metadata = response.json()['causal-metadata']


    def test_availability(self):
        '''Does your store remain available when nodes are removed?'''
        metadata = None
    
        remaining = alice
        disconnected_replicas = sorted(set(all_replicas) - set([remaining]))

        for replica in disconnected_replicas:
            print('>>> Disconnect replica {}'.format(replica))
            disconnectFromNetwork(replica)

        print('... Wait for stabilization')
        sleep(1)

        print('>>> Put pop:tarts into the store')

        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, remaining.host_port, 'pop'),
                json={'value':'tarts', 'causal-metadata': metadata})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'created')
        metadata = response.json()['causal-metadata']


    def test_view_changes(self):
        metadata = None

        print('=== Check replica views')
        for replica in all_replicas:
            response = requests.get('http://{}:{}/view'.format(hostname, replica.host_port))
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('view', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(set(response.json()['view']), viewSet(all_replicas), msg='at replica, {}'.format(replica))


        print('>>> Put apple:strudel into the store')

        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'apple'),
                json={'value':'strudel', 'causal-metadata': metadata})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'created')
        metadata = response.json()['causal-metadata']

        print('... Wait for replication')
        sleep(5)

        print('=== Check apple:strudel at replicas {}'.format(','.join(r.name for r in all_replicas)))
        for replica in all_replicas:
            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, replica.host_port, 'apple'),
                    json={'causal-metadata':metadata})
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('result', response.json(), msg='at replica, {}'.format(replica))
            self.assertIn('causal-metadata', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(response.json()['value'], 'strudel', msg='at replica, {}'.format(replica))
            metadata = response.json()['causal-metadata']


        removed = carol
        remaining_replicas = sorted(set(all_replicas) - set([removed]))
        print('>>> Remove replica {}'.format(removed))
        killInstance(removed)

        print('... Wait for stabilization')
        sleep(5)


        print('>>> Put chocolate:eclair into the store')
        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'chocolate'),
                json={'value':'eclair', 'causal-metadata':metadata})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'created')
        metadata = response.json()['causal-metadata']

        print('... Wait for replication, down detection, and stabilization')
        sleep(5)

        print('=== Check chocolate:eclair at replicas {}'.format(','.join(r.name for r in remaining_replicas)))
        for replica in remaining_replicas:
            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, replica.host_port, 'chocolate'),
                json={'causal-metadata':metadata})
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('result', response.json(), msg='at replica, {}'.format(replica))
            self.assertIn('causal-metadata', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(response.json()['value'], 'eclair', msg='at replica, {}'.format(replica))
            metadata = response.json()['causal-metadata']

        print('=== Check view at replicas {}'.format(','.join(r.name for r in remaining_replicas)))
        for replica in remaining_replicas:
            response = requests.get('http://{}:{}/view'.format(hostname, replica.host_port))
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('view', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(set(response.json()['view']), viewSet(remaining_replicas), msg='at replica, {}'.format(replica))


        print('>>> Start replica {}'.format(removed))
        runReplica(removed, all_replicas)

        print('... Wait for stabilization')
        sleep(5)


        print('=== Check replica views')
        for replica in all_replicas:
            response = requests.get('http://{}:{}/view'.format(hostname, replica.host_port))
            self.assertEqual(response.status_code, 200, msg='at replica, {}'.format(replica))
            self.assertIn('view', response.json(), msg='at replica, {}'.format(replica))
            self.assertEqual(set(response.json()['view']), viewSet(all_replicas), msg='at replica, {}'.format(replica))


        print('=== Check keys apple,chocolate at replicas {}'.format(removed.name))
        for key,val in {'apple':'strudel', 'chocolate':'eclair'}.items():
            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, removed.host_port, key),
                json={'causal-metadata':metadata})
            self.assertEqual(response.status_code, 200, msg='for key, {}'.format(key))
            self.assertIn('result', response.json(), msg='for key, {}'.format(key))
            self.assertIn('causal-metadata', response.json(), msg='for key, {}'.format(key))
            self.assertEqual(response.json()['value'], val, msg='for key, {}'.format(key))
            metadata = response.json()['causal-metadata']


    def test_causal_consistency(self):
        '''Does your store prevent violations of causal consistency?'''
        metadata = None


        print('>>> Put matcha:ice-cream into the store')
        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'matcha'),
                json={'value':'ice-cream', 'causal-metadata':metadata})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'created')
        metadata = response.json()['causal-metadata']
        previous_metadata = metadata


        print('>>> Disconnect replica {}'.format(bob))
        disconnectFromNetwork(bob)

        print('... Wait for stabilization')
        sleep(1)

        print('>>> Put matcha:tea into the store')
        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'matcha'),
                json={'value':'tea', 'causal-metadata':metadata})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['result'], 'replaced')
        metadata = response.json()['causal-metadata']

        print('>>> Connect replica {}'.format(bob))
        connectToNetwork(bob)

        print('... Wait for stabilization')
        sleep(1)


        print('=== Check matcha at replica alice')
        response = requests.get('http://{}:{}/kvs/{}'.format(hostname, alice.host_port, 'matcha'),
                json={'causal-metadata':metadata})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['value'], 'tea')
        metadata = response.json()['causal-metadata']


        print('=== Check matcha at replica bob (it fails)')
        response = requests.get('http://{}:{}/kvs/{}'.format(hostname, bob.host_port, 'matcha'),
                json={'causal-metadata':metadata})
        self.assertEqual(response.status_code, 503)
        self.assertIn('error', response.json())

        print('>>> Put matcha:mochi into the store at bob (it fails)')
        response = requests.put('http://{}:{}/kvs/{}'.format(hostname, bob.host_port, 'matcha'),
                json={'value':'tea', 'causal-metadata':metadata})
        self.assertEqual(response.status_code, 503)
        self.assertIn('error', response.json())

        print('=== Check matcha at replica bob with old metadata')
        response = requests.get('http://{}:{}/kvs/{}'.format(hostname, bob.host_port, 'matcha'),
                json={'causal-metadata':previous_metadata})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('causal-metadata', response.json())
        self.assertEqual(response.json()['value'], 'ice-cream', msg='bob is able to respond')
        metadata = response.json()['causal-metadata']


if __name__ == '__main__':
    try:
        buildDockerImage()
    except KeyboardInterrupt:
        TestHW3.setUpClass()
        TestHW3.tearDownClass()
    unittest.main(verbosity=0)
