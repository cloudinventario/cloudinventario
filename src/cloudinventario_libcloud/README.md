# LibCloud

# Config

* key
* secret
* driver
    * vm
    * lb
    * storage
    * container
    * dns
* driver_params

# Collecting for VMs (type: vm)

* id
* created
* name
* size
* image
* cluster
* project
* primary_ip
* public_ip
* private_ip
* status
* is_on
* tags

# Collecting for LoadBalancer (type: lb)

* id
* name
* cluster
* project
* ip
* port
* instances
* status
* tags
 
# Collecting for Storage (type: storage)

* name
* driver
* cluster
* project
* objects

# Collecting for Container (type: container)

* id
* name
* cluster
* project
* state
* image
* public_ip
* is_on

# Collecting for DNS (type: dns)

## dns_domain
* id
* domain
* domain_type
* ttl

## dns_record
* id
* name
* record_type
* data
* domain
* ttl