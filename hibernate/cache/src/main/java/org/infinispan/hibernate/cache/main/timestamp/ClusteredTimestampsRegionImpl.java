package org.infinispan.hibernate.cache.main.timestamp;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;

public class ClusteredTimestampsRegionImpl
   extends org.infinispan.hibernate.cache.commons.timestamp.ClusteredTimestampsRegionImpl
   implements TimestampsRegion {

   public ClusteredTimestampsRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);
   }

   @Override
   public Object get(SharedSessionContractImplementor session, Object key) throws CacheException {
      return getItem(session, key);
   }

   @Override
   public void put(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      putItem(session, key, value);
   }

}
