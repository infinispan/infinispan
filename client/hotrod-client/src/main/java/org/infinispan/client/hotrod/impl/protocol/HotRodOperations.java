package org.infinispan.client.hotrod.impl.protocol;


import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.BinaryVersionedValue;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;

import java.util.Map;

/**
 * Defines the hotrod operations as described in the protocol spec: http://community.jboss.org/wiki/HotRodProtocol
 *
 * - TODO - enforce encoding and add such tests
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodOperations {

   public byte[] get(byte[] key, Flag... flags);

   public byte[] remove(byte[] key, Flag... flags);

   public boolean containsKey(byte[] key, Flag... flags);

   /**
    * Returns null if the given key does not exist.
    */
   public BinaryVersionedValue getWithVersion(byte[] key, Flag... flags);

   /**
    * @param lifespan number of seconds that a entry during which the entry is allowed to life.
    * If number of seconds is bigger than 30 days, this number of seconds is treated as UNIX time and so, represents
    * the number of seconds since 1/1/1970. If set to 0, lifespan is unlimited.
    * @param maxIdle Number of seconds that a entry can be idle before it's evicted from the cache. If 0, no max
    * @param flags
    */
   public byte[] put(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags);

   /**
    * @param lifespan same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param maxIdle same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param flags
    */
   public byte[] putIfAbsent(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags);

   /**
    * @param lifespan same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param maxIdle same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param flags
    */
   public byte[] replace(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags);

   /**
    * @param lifespan same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param maxIdle same as in {@link #put(byte[],byte[],int,int,org.infinispan.client.hotrod.Flag...)}
    * @param flags
    */
   public VersionedOperationResponse replaceIfUnmodified(byte[] key, byte[] value, int lifespan, int maxIdle, long version, Flag... flags);

   public VersionedOperationResponse removeIfUnmodified(byte[] key, long version, Flag... flags);

   public void clear(Flag... flags);

   public Map<String, String> stats();

   public boolean ping();
}
