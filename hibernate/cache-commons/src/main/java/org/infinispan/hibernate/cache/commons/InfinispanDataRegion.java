package org.infinispan.hibernate.cache.commons;

import java.util.Comparator;

public interface InfinispanDataRegion extends InfinispanBaseRegion {

   long getTombstoneExpiration();

   Comparator<Object> getComparator(String subclass);
}
