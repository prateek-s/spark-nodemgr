from __future__ import with_statement

import boto
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import urllib2
import socket
import thread
from optparse import OptionParser
from sys import stderr
from datetime import datetime, timedelta
from boto.ec2.blockdevicemapping import BlockDeviceMapping, EBSBlockDeviceType

# global variables
# instances that are being terminated, used to ignore notifications sent from these instances
terminating_instances=[]
cluster_changing= False

def parse_args():
  parser = OptionParser(usage="node_manager [options] <action> <cluster_name>"
      + "\n\n<action> can be: launch, destroy, stop, start, manage",
      add_help_option=False)
  parser.add_option("-h", "--help", action="help",
                    help="Show this help message and exit")
  parser.add_option("-c", "--core-slaves", type="int", default=1,
      help="Number of core slaves to launch (default: 1)")
  parser.add_option("-p", "--policy", default="migration",
      help="Number of core slaves to launch (default: 1)")
  parser.add_option("-g", "--master-group", default="default",
      help="Master Group Name (default: default)")
  parser.add_option("-l", "--task-slaves", type="int", default=0,
      help="Number of task slaves to launch (default: 0)")
  parser.add_option("-w", "--wait", type="int", default=100,
      help="Number of seconds to wait for cluster nodes to start (default: 60)")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-o", "--public-key", 
      help="SSH public key file to use for logging into instances")
  parser.add_option("-i", "--identity-file", 
      help="SSH private key file to use for logging into instances")
  parser.add_option("-t", "--instance-type", default="m3.medium",
      help="Type of instance to launch (default: m3.large). " +
           "WARNING: must be 64 bit, thus small instances won't work")
  parser.add_option("-m", "--master-ip", default="",
      help="Master instance ip address")
  parser.add_option("-z", "--zone", default="us-east-1e",
      help="Availability zone to launch instances in")
  parser.add_option("-a", "--ami", default="ami-a23f24ca",
      help="Amazon Machine Image ID to use, or 'latest' to use latest " +
           "availabe AMI (default: ami-4517dc2c)")
  parser.add_option("--spot-price", metavar="PRICE", type="float",
      help="If specified, launch slaves as spot instances with the given " +
            "maximum price (in dollars)")
  
  (opts, args) = parser.parse_args()
  if len(args) != 2:
    parser.print_help()
    sys.exit(1)
  (action, cluster_name) = args
  if opts.identity_file == None and action in ['launch', 'login']:
    print >> stderr, ("ERROR: The -i or --identity-file argument is " +
                      "required for " + action)
    sys.exit(1)
  if opts.master_ip == None:
    print >> stderr, ("ERROR: The -m or --master-ip argument is " +
                      "required")
    sys.exit(1)
  if os.getenv('AWS_ACCESS_KEY_ID') == None:
    print >> stderr, ("ERROR: The environment variable AWS_ACCESS_KEY_ID " +
                      "must be set")
    sys.exit(1)
  if os.getenv('AWS_SECRET_ACCESS_KEY') == None:
    print >> stderr, ("ERROR: The environment variable AWS_SECRET_ACCESS_KEY " +
                      "must be set")
    sys.exit(1)
  return (opts, action, cluster_name)

# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
  groups = conn.get_all_security_groups()
  group = [g for g in groups if g.name == name]
  if len(group) > 0:
    return group[0]
  else:
    print "Creating security group " + name
    return conn.create_security_group(name, "Mesos EC2 group")
    
# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
  while True:
    for i in instances:
      i.update()
    if len([i for i in instances if i.state == 'pending']) > 0:
      time.sleep(5)
    else:
      return

# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master, slave
# and zookeeper instances (in that order).
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
  print "Setting up security groups..."
  master_group = get_or_make_group(conn, opts.master_group)
  core_slave_group = get_or_make_group(conn, cluster_name + "-core-slaves")
  task_slave_group = get_or_make_group(conn, cluster_name + "-task-slaves")
  if master_group.rules == []: # Group was just now created
    master_group.authorize(src_group=master_group)
    master_group.authorize(src_group=core_slave_group)
    master_group.authorize(src_group=task_slave_group)
    master_group.authorize('tcp', 22, 22, '0.0.0.0/0')
    master_group.authorize('tcp', 8080, 8081, '0.0.0.0/0')
    master_group.authorize('tcp', 50030, 50030, '0.0.0.0/0')
    master_group.authorize('tcp', 50070, 50070, '0.0.0.0/0')
    master_group.authorize('tcp', 60070, 60070, '0.0.0.0/0')
    master_group.authorize('tcp', 38090, 38090, '0.0.0.0/0')
  if core_slave_group.rules == []: # Group was just now created
    core_slave_group.authorize(src_group=master_group)
    core_slave_group.authorize(src_group=core_slave_group)
    core_slave_group.authorize(src_group=task_slave_group)
    core_slave_group.authorize('tcp', 22, 22, '0.0.0.0/0')
    core_slave_group.authorize('tcp', 8080, 8081, '0.0.0.0/0')
    core_slave_group.authorize('tcp', 50060, 50060, '0.0.0.0/0')
    core_slave_group.authorize('tcp', 50075, 50075, '0.0.0.0/0')
    core_slave_group.authorize('tcp', 60060, 60060, '0.0.0.0/0')
    core_slave_group.authorize('tcp', 60075, 60075, '0.0.0.0/0')
  if task_slave_group.rules == []: # Group was just now created
    task_slave_group.authorize(src_group=master_group)
    task_slave_group.authorize(src_group=core_slave_group)
    task_slave_group.authorize(src_group=task_slave_group)
    task_slave_group.authorize('tcp', 22, 22, '0.0.0.0/0')
    task_slave_group.authorize('tcp', 2181, 2181, '0.0.0.0/0')
    task_slave_group.authorize('tcp', 2888, 2888, '0.0.0.0/0')
    task_slave_group.authorize('tcp', 3888, 3888, '0.0.0.0/0')
  # Check if instances are already running in our groups
  print "Checking for running cluster..."
  reservations = conn.get_all_instances()
  for res in reservations:
    group_names = [g.name for g in res.groups]
    if core_slave_group.name in group_names or task_slave_group.name in group_names:
      active = [i for i in res.instances if is_active(i)]
      if len(active) > 0:
        print >> stderr, ("ERROR: There are already instances running in " +
            "group %s, %s or %s" % (core_slave_group.name, task_slave_group.name))
        sys.exit(1)
  print "Launching instances..."
  
  try:
    image = conn.get_all_images(image_ids=[opts.ami])[0]
  except:
    print >> stderr, "Could not find AMI " + opts.ami
    sys.exit(1)
  
  # Launch core slaves
  core_slave_nodes=[]
  if opts.core_slaves>0:
    core_slave_res = image.run(key_name = opts.key_pair,
                        security_groups = [core_slave_group],
                        instance_type = opts.instance_type,
                        placement = opts.zone,
                        min_count = opts.core_slaves,
                        max_count = opts.core_slaves,
                        block_device_map = BlockDeviceMapping())
    core_slave_nodes = core_slave_res.instances
    print "Launched core slaves, regid = " + core_slave_res.id
 
  
  # Launch task slaves
  task_slave_nodes = []
  if opts.task_slaves>0:
    if opts.spot_price != None:
    # Launch spot instances with the requested price
      print ("Requesting %d task slaves as spot instances with price $%.3f" %
           (opts.task_slaves, opts.spot_price))
      task_slave_reqs = conn.request_spot_instances(
        price = opts.spot_price,
        image_id = opts.ami,
        launch_group = "launch-group-%s" % cluster_name,
        placement = opts.zone,
        count = opts.task_slaves,  
        key_name = opts.key_pair,
        security_groups = [task_slave_group],
        instance_type = opts.instance_type,
        block_device_map = BlockDeviceMapping())
      my_req_ids = [req.id for req in task_slave_reqs]
      print "Waiting for spot instances to be granted..."
      while True:
        time.sleep(10)
        reqs = conn.get_all_spot_instance_requests()
        id_to_req = {}
        for r in reqs:
          id_to_req[r.id] = r
        active = 0
        instance_ids = []
        for i in my_req_ids:
          if id_to_req[i].state == "active":
            active += 1
            instance_ids.append(id_to_req[i].instance_id)
        if active == opts.task_slaves:
          print "All %d task slaves granted" % opts.task_slaves
          reservations = conn.get_all_instances(instance_ids)
          for r in reservations:
            task_slave_nodes += r.instances
          break
        else:
          print "%d of %d task_slaves granted, waiting longer" % (active, opts.task_slaves)
    else:
    # Launch non-spot instances
      task_slave_res = image.run(key_name = opts.key_pair,
                          security_groups = [task_slave_group],
                          instance_type = opts.instance_type,
                          placement = opts.zone,
                          min_count = opts.task_slaves,
                          max_count = opts.task_slaves,
                          block_device_map = BlockDeviceMapping())
      task_slave_nodes = task_slave_res.instances
      print "Launched task slaves, regid = " + task_slave_res.id
  return (core_slave_nodes, task_slave_nodes)
  
  # Get the EC2 instances in an existing cluster if available.
# Returns a tuple of lists of EC2 instance objects for the masters,
# slaves and zookeeper nodes (in that order).
def get_existing_task_nodes(conn, opts, instance_type, cluster_name):
  print "Searching for %s task nodes in cluster " % instance_type + cluster_name + "..." 
  reservations = conn.get_all_instances()
  task_slave_nodes = []
  for res in reservations:
    active = [i for i in res.instances if is_active(i)]
    if len(active) >0:
      group_names = [g.name for g in res.groups]
      if group_names ==[cluster_name + "-task-slaves"]:
        for ins in res.instances:
          if ins.instance_type == instance_type:
            task_slave_nodes += ins	
  return task_slave_nodes
    
def get_existing_cluster(conn, opts, cluster_name):
  print "Searching for existing cluster " + cluster_name + "..."
  reservations = conn.get_all_instances()
  core_slave_nodes = []
  task_slave_nodes = []
  for res in reservations:
    active = [i for i in res.instances if is_active(i)]
    if len(active) > 0:
      group_names = [g.name for g in res.groups]
      if group_names == [cluster_name + '-core-slaves']:
        core_slave_nodes += res.instances
      elif group_names == [cluster_name + '-task-slaves']:
        task_slave_nodes += res.instances
  if core_slave_nodes != [] or task_slave_nodes != []:
    print ("Found %d core_slave(s), %d task_slaves" %
           (len(core_slave_nodes), len(task_slave_nodes)))
    return (core_slave_nodes, task_slave_nodes)
  else:
    print "ERROR: Could not find any existing cluster"
    sys.exit(1)
    
# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
# TODO
def setup_cluster(conn, core_slave_nodes, task_slave_nodes, opts, deploy_ssh_key):
  #print "Deploying files to master..."
  #deploy_files(conn, "deploy." + opts.os, opts, master_nodes, slave_nodes, zoo_nodes)
  #master = master_nodes[0].public_dns_name
  if deploy_ssh_key:
    subprocess.check_call('cat /home/ubuntu/.ssh/id_rsa.pub >> /home/ubuntu/.ssh/authorized_keys', shell=True)
    for i in core_slave_nodes:
      print "Copying SSH key %s to node %s..." % (opts.public_key, i.private_dns_name)
      ssh(i.public_dns_name, opts, 'mkdir -p /home/ubuntu/.ssh')
      scp(i.public_dns_name, opts, opts.public_key, '/home/ubuntu/.ssh/id_rsa.pub')
      ssh(i.public_dns_name, opts, 'cat /home/ubuntu/.ssh/id_rsa.pub >> /home/ubuntu/.ssh/authorized_keys')
      ssh(i.private_dns_name, opts, "sed -i 's/master_ip/%s/g' /home/ubuntu/client.py" % 
       opts.master_ip)
  #print "Running setup on master..."
  #ssh(master, opts, "chmod u+x mesos-ec2/setup")
  #ssh(master, opts, "mesos-ec2/setup %s %s %s %s" %
  #    (opts.os, opts.download, opts.branch, opts.swap))
  print "Done!"

# Wait for a whole cluster (core-slaves and task-slaves) to start up
def wait_for_cluster(conn, wait_secs, core_slave_nodes, task_slave_nodes):
  print "Waiting for instances to start up..."
  time.sleep(5)
  wait_for_instances(conn, core_slave_nodes)
  wait_for_instances(conn, task_slave_nodes)    
  print "Waiting %d more seconds..." % wait_secs
  time.sleep(wait_secs)  
  

def get_num_cores(instance_type):
  cores_by_instance = {
    "m3.medium":    1,
    "m3.large":    2,
    "m3.xlarge":   4,
    "m3.2xlarge":   8,
    "c4.large":  2,
    "c4.xlarge":  4,
    "c4.2xlarge":  8,
    "c4.4xlarge":  16,
    "c4.8xlarge":  36,
    "c3.large":  2,
    "c3.xlarge":  4,
    "c3.2xlarge":  8,
    "c3.4xlarge":  16,
    "c3.8xlarge":  32,
    "r3.large":  2,
    "r3.xlarge":  4,
    "r3.2xlarge":  8,
    "r3.4xlarge":  16,
    "r3.8xlarge":  32,
    "i2.xlarge":  4,
    "i2.2xlarge":  8,
    "i2.4xlarge":  16,
    "i2.8xlarge":  32,
  }
  if instance_type in cores_by_instance:
    return cores_by_instance[instance_type]
  else:
    print >> stderr, ("WARNING: Don't know number of cores on instance type %s; assuming 1"
                      % instance_type)
    return 1

def get_price(instance_type):
  price_by_instance = {
    "m3.medium":    0.07,
    "m3.large":    0.14,
    "m3.xlarge":   0.28,
    "m3.2xlarge":   0.56,
    "c4.large":  0.116,
    "c4.xlarge":  0.232,
    "c4.2xlarge":  0.464,
    "c4.4xlarge":  0.928,
    "c4.8xlarge":  1.856,
    "c3.large":  0.105,
    "c3.xlarge":  0.21,
    "c3.2xlarge":  0.42,
    "c3.4xlarge":  0.84,
    "c3.8xlarge":  1.68,
    "r3.large":  0.175,
    "r3.xlarge":  0.35,
    "r3.2xlarge":  0.7,
    "r3.4xlarge":  1.4,
    "r3.8xlarge":  2.8,
    "i2.xlarge":  0.853,
    "i2.2xlarge":  1.705,
    "i2.4xlarge":  3.41,
    "i2.8xlarge":  6.82,
  }
  if instance_type in price_by_instance:
    return price_by_instance[instance_type]
  else:
    print >> stderr, ("WARNING: Don't know price of instance type %s; assuming 1"
                      % instance_type)
    return 0

# Configure HDFS
def configHDFS(opts, core_slave_nodes):
  subprocess.check_call("sed -i 's/master_ip/%s/g' /home/ubuntu/hadoop-1.2.1/conf/core-site.xml" % 
  (opts.master_ip) , shell=True)
  output=open('/home/ubuntu/hadoop-1.2.1/conf/masters','w')
  output.write(opts.master_ip)
  output=open('/home/ubuntu/hadoop-1.2.1/conf/slaves','w')
  for i in core_slave_nodes:
    output.write(i.private_dns_name+'\n')
    ssh(i.private_dns_name, opts, 'sudo chmod 777 /mnt')
    ssh(i.private_dns_name, opts, "sed -i 's/master_ip/%s/g' /home/ubuntu/hadoop-1.2.1/conf/core-site.xml" % 
    opts.master_ip)
  output.flush()
  subprocess.check_call("sudo chmod 777 /mnt", shell=True)
  subprocess.check_call("/home/ubuntu/hadoop-1.2.1/bin/hadoop namenode -format", shell=True)

# Start HDFS
def startHDFS():
  subprocess.check_call(
	  "bash /home/ubuntu/hadoop-1.2.1/bin/start-dfs.sh", shell=True)
  print "HDFS has been started, you can copy files now"
	  
def startMesosMaster(opts):
  subprocess.check_call(
	  "nohup /home/ubuntu/mesos/build/bin/mesos-master.sh --ip=%s --work_dir=/mnt/mesos >/home/ubuntu/mesos/masterlog &" % opts.master_ip , shell=True)
	  
def startMesosSlaves(opts, slave_nodes):
  for i in slave_nodes:
    ssh(i.private_ip_address, opts, "nohup /home/ubuntu/mesos/build/bin/mesos-slave.sh --master=%s:5050 &>/home/ubuntu/mesos/slavelog &" % opts.master_ip)

	
# Copy a file to a given host through scp, throwing an exception if scp fails
def scp(host, opts, local_file, dest_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' 'ubuntu@%s:%s'" %
      (opts.identity_file, local_file, host, dest_file), shell=True)


# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, opts, command):
  subprocess.check_call(
      "ssh -o StrictHostKeyChecking=no -i %s ubuntu@%s '%s'" %
      (opts.identity_file, host, command), shell=True)

def getSpotPrice(conn, opts):
  date=datetime.now()+timedelta(days = -1)
  results = conn.get_spot_price_history(date.isoformat(), None, None, "Linux/UNIX", opts.zone)
  prices={}
  for i in results:
    if i.instance_type not in prices:
	  prices[i.instance_type]=i.price
  return prices

# terminate a list of slave nodes, get_existing_task_nodes can be used to get task nodes of a specific type, get_existing_cluster can be 
# used to get all task nodes  
# use something like task_slave_nodes-=slave_nodes after this method to remove instances from task_slave_nodes
def removeTaskSlaves(conn, opts, slave_nodes):
  global terminating_instances
  terminating_instances+=slave_nodes
  conn.terminate_instances([i.id for i in slave_nodes])

# this is used to launch task slave nodes, if you want to start mesos, please use startMesosSlaves
# use something like task_slave_nodes+=addTaskSlaves(blabla) to add instances into task_slave_nodes
def addTaskSlaves(conn, opts, instance_type, num_slaves , bidding = None):
  slave_nodes = []
  if num_slaves>0:
    if bidding != None:
    # Launch spot instances with the requested price
      print ("Requesting %d task slaves as spot instances with price $%.3f" %
           (num_slaves, bidding))
      task_slave_reqs = conn.request_spot_instances(
        price = bidding,
        image_id = opts.ami,
        launch_group = "launch-group-%s" % cluster_name,
        placement = opts.zone,
        count = num_slaves,
        key_name = opts.key_pair,
        security_groups = [task_slave_group],
        instance_type = instance_type,
        block_device_map = BlockDeviceMapping())
      my_req_ids = [req.id for req in task_slave_reqs]
      print "Waiting for spot instances to be granted..."
      while True:
        time.sleep(10)
        reqs = conn.get_all_spot_instance_requests()
        id_to_req = {}
        for r in reqs:
          id_to_req[r.id] = r
        active = 0
        instance_ids = []
        for i in my_req_ids:
          if id_to_req[i].state == "active":
            active += 1
            instance_ids.append(id_to_req[i].instance_id)
        if active == num_slaves:
          print "All %d task slaves granted" % num_slaves
          reservations = conn.get_all_instances(instance_ids)
          for r in reservations:
            slave_nodes += r.instances
          break
        else:
          print "%d of %d task_slaves granted, waiting longer" % (active, num_slaves)
    else:
    # Launch non-spot instances
      task_slave_res = image.run(key_name = opts.key_pair,
                          security_groups = [task_slave_group],
                          instance_type = instance_type,
                          placement = opts.zone,
                          min_count = num_slaves,
                          max_count = num_slaves,
                          block_device_map = BlockDeviceMapping())
      slave_nodes = task_slave_res.instances
      print "Launched task slaves, regid = " + task_slave_res.id
  return slave_nodes
		
def listenNotification(conn, opts, task_slave_nodes):
  global cluster_changing
  global terminating_instances
  s = socket.socket()         # Create a socket object
  host = socket.gethostname() # Get local machine name
  port = 12345                # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port
  s.listen(5)                 # Now wait for client connection.
  while True:
    c, (addr, pt) = s.accept()     # Establish connection with client.
    print 'Got notification from' + addr
    c.send('Thank you for connecting')
    c.close()                # Close the connection
    while addr not in [i.private_ip_address for i in terminating_instances] and addr in [i.private_ip_address for i in task_slave_nodes]:
      if cluster_changing==False:
        cluster_changing=True
        update_status(opts, task_slave_nodes)
        # Make decisions here
        for t in task_slave_nodes:
          if t.state in ["shutting-down", "terminated"]:
            task_slave_nodes.remove(t)    
          cluster_changing=False
      else:
        time.sleep(5)
	
def update_status(opts, slave_nodes):
  for i in slave_nodes:
    i.update()
    
def manageCluster(conn, opts, core_slave_nodes, task_slave_nodes):
  global cluster_changing
  while True:
	# check current spot prices
    prices=getSpotPrice(conn, opts)
    # update instance status and remove terminated instances
    if cluster_changing == False:
      cluster_changing=True
      update_status(opts, task_slave_nodes)
    # Make decisions
    
    # Remove task nodes from task slave list
    for t in task_slave_nodes:
      if t.state in ["shutting-down", "terminated"]:
        task_slave_nodes.remove(t)
    
    cluster_changing=False
    time.sleep(60) # check price and cluster status every min

def main(): 
  (opts, action, cluster_name) = parse_args()
  conn = boto.connect_ec2()
  if action == "launch":
    (core_slave_nodes, task_slave_nodes) = launch_cluster(
          conn, opts, cluster_name)
    wait_for_cluster(conn, opts.wait, core_slave_nodes, task_slave_nodes)
    setup_cluster(conn, core_slave_nodes, task_slave_nodes, opts, True)
    #print "starting HDFS..."
    #configHDFS(opts, core_slave_nodes)
    #startHDFS()
    #print "starting Mesos..."
    #startMesosMaster(opts)
    #startMesosSlaves(opts, core_slave_nodes)
    #startMesosSlaves(opts, task_slave_nodes)
    #manageCluster(opts, task_slave_nodes)
  if action == "start":
    (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print "starting HDFS..."
    configHDFS(opts, core_slave_nodes)
    startHDFS()
    print "starting Mesos..."
    startMesosMaster(opts)
    startMesosSlaves(opts, core_slave_nodes)
    startMesosSlaves(opts, task_slave_nodes)
    #manageCluster(opts, task_slave_nodes)
  if action == "manage":
    print "Managing Cluster"
    (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    thread.start_new_thread(manageCluster, (conn, opts, core_slave_nodes, task_slave_nodes))
    thread.start_new_thread(listenNotification, (conn, opts, task_slave_nodes))
  if action == "stopall":
    response = raw_input("Are you sure you want to stop the cluster " +
        cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
        "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" + 
        "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
        "Stop cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Stopping core slaves..."
      for inst in core_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.stop()
      print "Stopping task slaves..."
      for inst in task_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.stop()
  if action == "stopcore":
    response = raw_input("Are you sure you want to stop the core nodes in cluster " +
        cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
        "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" + 
        "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
        "Stop cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Stopping core slaves..."
      for inst in core_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.stop()
  if action == "stoptask":
    response = raw_input("Are you sure you want to stop the task nodes in cluster " +
        cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
        "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" + 
        "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
        "Stop cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Stopping task slaves..."
      for inst in task_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.stop()
  if action == "terminateall":
    response = raw_input("Are you sure you want to destroy the cluster " +
        cluster_name + "?\nALL DATA ON ALL NODES WILL BE LOST!!\n " +
        "Destory cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Terminating core slaves..."
      for inst in core_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.terminate()
      print "Terminating task slaves..."
      for inst in task_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.terminate()
  if action == "terminatetask":
    response = raw_input("Are you sure you want to destroy task nodes of the cluster " +
        cluster_name + "?\nALL DATA ON Task NODES WILL BE LOST!!\n " +
        "Destory cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Terminating task slaves..."
      for inst in task_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.terminate()
  if action == "terminatecore":
    response = raw_input("Are you sure you want to destroy core nodes of the cluster " +
        cluster_name + "?\nALL DATA ON Core NODES WILL BE LOST!!\n " +
        "Destory cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
      print "Terminating core slaves..."
      for inst in core_slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.terminate()
  if action == "resumeall":
    (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print "Resuming core slaves..."
    for inst in core_slave_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
    print "Resuming task slaves..."
    for inst in task_slave_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
  if action == "resumecore":
    (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print "Resuming core slaves..."
    for inst in core_slave_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
  if action == "resumetask":
    (core_slave_nodes, task_slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print "Resuming task slaves..."
    for inst in task_slave_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
if __name__ == "__main__":
  logging.basicConfig()
  main()
