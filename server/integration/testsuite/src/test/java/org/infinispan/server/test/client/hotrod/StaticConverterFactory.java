package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.Converter;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

@NamedFactory(name = "static-converter-factory")
public class StaticConverterFactory implements ConverterFactory {
    @Override
    public Converter<Integer, String, CustomEvent> getConverter(Object[] params) {
        return new StaticConverter();
    }

    static class StaticConverter implements Converter<Integer, String, CustomEvent>, Serializable {
        @Override
        public CustomEvent convert(Integer key, String value, Metadata metadata) {
            return new CustomEvent(key, value);
        }
    }
}
