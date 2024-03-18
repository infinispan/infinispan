package org.infinispan.cache.impl;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.Query;

public interface QueryProducer {

   <T> Query<T> query(String query);

   <K, V> ContinuousQuery<K, V> continuousQuery(Cache<K, V> cache);

}
