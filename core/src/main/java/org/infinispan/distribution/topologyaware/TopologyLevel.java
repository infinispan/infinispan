package org.infinispan.distribution.topologyaware;

/**
 * The cluster topology is a tree with five levels: the entire cluster, sites, racks, machines, and
 * individual nodes.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public enum TopologyLevel {
   NODE,
   MACHINE,
   RACK,
   SITE,
}
