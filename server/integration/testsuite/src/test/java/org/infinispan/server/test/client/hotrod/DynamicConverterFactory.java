package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.Converter;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

@NamedFactory(name = "dynamic-converter-factory")
public class DynamicConverterFactory implements ConverterFactory {
    @Override
    public Converter<Integer, String, CustomEvent> getConverter(final Object[] params) {
        return new Converter<Integer, String, CustomEvent>() {
            @Override
            public CustomEvent convert(Integer key, String value, Metadata metadata) {
                if (params[0].equals(key))
                    return new CustomEvent(key, null);

                return new CustomEvent(key, value);
            }
        };
    }
}
