#!/bin/bash

declare -a replica_floating_ips 
declare -a replica_ips
declare -a client_floating_ips

replica_floating_ips=(
  129.114.108.152 
  129.114.109.38 
  129.114.108.219 
  129.114.108.181
)

replica_ips=(0 0 0 0)

ttp_ip="7001 127.0.0.1 11100 11101"

