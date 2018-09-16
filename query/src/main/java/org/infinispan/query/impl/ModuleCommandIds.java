package org.infinispan.query.impl;

/**
 * The Query module is using custom RPC commands; to make sure the used command ids
 * are unique all numbers are defined here, and should stay in the range 100-119
 * which is the reserved range for this module.
 *
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
public interface ModuleCommandIds {

   byte CLUSTERED_QUERY = 101;

   byte UPDATE_INDEX = 102;

   byte UPDATE_INDEX_STREAM = 103;

   byte UPDATE_INDEX_AFFINITY = 104;
}
