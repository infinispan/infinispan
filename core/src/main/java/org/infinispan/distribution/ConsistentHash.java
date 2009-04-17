package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A consistent hash algorithm
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ConsistentHash {

   void setCaches(Collection<Address> caches);

   List<Address> locate(Object key, int replCount);

   Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount);

}
