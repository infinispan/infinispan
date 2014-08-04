package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

@NamedFactory(name = "static-filter-factory")
public class StaticKeyValueFilterFactory implements KeyValueFilterFactory {
   final Integer staticKey = 2;
   @Override
   public KeyValueFilter<Integer, String> getKeyValueFilter(final Object[] params) {
      return new KeyValueFilter<Integer, String>() {
         @Override
         public boolean accept(Integer key, String value, Metadata metadata) {
            return staticKey.equals(key);
         }
      };
   }
}
