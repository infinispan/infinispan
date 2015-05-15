package org.infinispan.filter;

/**
 * Factory for {@link org.infinispan.filter.KeyValueFilterConverter} instances
 *
 * @author gustavonalle
 * @since 8.0
 */
public interface KeyValueFilterConverterFactory<K, V, C> {

   KeyValueFilterConverter<K, V, C> getFilterConverter();

}
