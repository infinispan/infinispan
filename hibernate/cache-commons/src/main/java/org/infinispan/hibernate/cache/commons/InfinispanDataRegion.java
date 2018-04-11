package org.infinispan.hibernate.cache.commons;

import java.util.Comparator;

import org.infinispan.functional.MetaParam;

public interface InfinispanDataRegion extends InfinispanBaseRegion {

   long getTombstoneExpiration();

   MetaParam.MetaLifespan getExpiringMetaParam();

   Comparator<Object> getComparator(String subclass);
}
