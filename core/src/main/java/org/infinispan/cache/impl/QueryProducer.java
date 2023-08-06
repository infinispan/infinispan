package org.infinispan.cache.impl;

import org.infinispan.commons.api.query.Query;

public interface QueryProducer {

   <T> Query<T> query(String query);

}
