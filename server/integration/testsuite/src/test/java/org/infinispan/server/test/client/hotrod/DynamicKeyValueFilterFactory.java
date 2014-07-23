package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

@NamedFactory(name = "dynamic-filter-factory")
public class DynamicKeyValueFilterFactory implements KeyValueFilterFactory {
    @Override
    public KeyValueFilter<Integer, String> getKeyValueFilter(final Object[] params) {
        return new DynamicKeyValueFilter(params);
    }

    static class DynamicKeyValueFilter implements KeyValueFilter<Integer, String>, Serializable {
        private final Object[] params;

        public DynamicKeyValueFilter(Object[] params) {
            this.params = params;
        }

        @Override
        public boolean accept(Integer key, String value, Metadata metadata) {
            return params[0].equals(key); // dynamic
        }
    }
}
