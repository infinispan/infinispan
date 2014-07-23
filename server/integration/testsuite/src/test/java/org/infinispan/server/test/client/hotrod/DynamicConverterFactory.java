package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.Converter;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

@NamedFactory(name = "dynamic-converter-factory")
public class DynamicConverterFactory implements ConverterFactory {
    @Override
    public Converter<Integer, String, CustomEvent> getConverter(final Object[] params) {
        return new DynamicConverter(params);
    }

    static class DynamicConverter implements Converter<Integer, String, CustomEvent>, Serializable {
        private final Object[] params;

        public DynamicConverter(Object[] params) {
            this.params = params;
        }

        @Override
        public CustomEvent convert(Integer key, String value, Metadata metadata) {
            if (params[0].equals(key))
                return new CustomEvent(key, null);

            return new CustomEvent(key, value);
        }
    }

}
