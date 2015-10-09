package org.infinispan.filter;

/**
 * Factory for {@link org.infinispan.filter.KeyValueFilterConverter} instances supporting
 * parameters
 *
 * @author gustavonalle
 * @since 8.1
 */
public interface ParamKeyValueFilterConverterFactory<K, V, C> extends KeyValueFilterConverterFactory {

   KeyValueFilterConverter<K, V, C> getFilterConverter(Object[] params);

   @Override
   default KeyValueFilterConverter getFilterConverter() {
      return getFilterConverter(null);
   }
}
