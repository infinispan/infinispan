package org.infinispan.hotrod.impl.cache;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.api.common.CacheOptions;

/**
 * @since 14.0
 **/
public class RemoteQuery {
   final String query;
   final Map<String, Object> params = new HashMap<>();
   Integer limit;
   Long skip;

   public RemoteQuery(String query, CacheOptions options) {
      this.query = query;
   }

   public void param(String name, Object value) {
      params.put(name, value);
   }

   public void limit(int limit) {
      this.limit = limit;
   }

   public void skip(long skip) {
      this.skip = skip;
   }

   public Object[] toFactoryParams() {
      if (params.isEmpty()) {
         return new Object[]{query};
      }
      Object[] factoryParams = new Object[1 + params.size() * 2];
      factoryParams[0] = query;
      int i = 1;
      for (Map.Entry<String, Object> e : params.entrySet()) {
         factoryParams[i++] = e.getKey();
         factoryParams[i++] = e.getValue();
      }
      return factoryParams;
   }
}
