package org.infinispan.query.impl;

/**
 * The Query module is using custom RPC commands; to make sure the used command ids
 * are unique all numbers are defined here, and should stay in the range 100-119
 * which is the reserved range for this module.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
public interface ModuleCommandIds {

   public static final byte CLUSTERED_QUERY = 101;

   public static final byte UPDATE_INDEX = 102;
   
   public static final byte UPDATE_INDEX_STREAM = 103;

}
