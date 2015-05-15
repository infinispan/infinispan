package org.infinispan.server.test.client.hotrod;

import java.io.Serializable;
import java.util.Arrays;

import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.NamedFactory;

/**
 * @author gustavonalle
 * @since 8.0
 */
@NamedFactory(name = "csv-key-value-filter-converter-factory")
public class TestKeyValueFilterConverterFactory implements KeyValueFilterConverterFactory {

   @Override
   public KeyValueFilterConverter<String, SampleEntity, Summary> getFilterConverter() {
      return new SampleKeyValueFilterConverter();
   }

   static class SampleKeyValueFilterConverter extends AbstractKeyValueFilterConverter<String, SampleEntity, Summary> implements Serializable {
      @Override
      public Summary filterAndConvert(String key, SampleEntity entity, Metadata metadata) {
         if ("ignore".equals(key)) {
            return null;
         }
         return new Summary(Arrays.asList(entity.getCsvAttributes().split(",")));
      }
   }

}
