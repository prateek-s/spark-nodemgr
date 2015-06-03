from datetime import datetime, timedelta
import boto

# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

def update_status(slave_nodes):
  for i in slave_nodes:
    i.update()

conn = boto.connect_ec2()
reservations = conn.get_all_instances()
task_slave_nodes = []
for res in reservations:
  task_slave_nodes += res.instances
print task_slave_nodes
update_status(task_slave_nodes)
for t in task_slave_nodes:
    if t.state in ["shutting-down", "stopped"]:
      task_slave_nodes.remove(t)
print task_slave_nodes
#cluster_name = "test-core-slaves"
#instance_type = "m3.medium"
#print "Searching for %s task nodes in cluster " % instance_type + cluster_name + "..." 
#reservations = conn.get_all_instances()
#task_slave_nodes = []

#for res in reservations:
#  active = [i for i in res.instances if is_active(i)]
#  if len(active) >0:
#    group_names = [g.name for g in res.groups]
#    print group_names
#    if group_names == [cluster_name]:
#      for ins in res.instances:
#        if ins.instance_type == instance_type:
#          task_slave_nodes += [ins]
#print task_slave_nodes
#conn.terminate_instances([i.id for i in task_slave_nodes])
