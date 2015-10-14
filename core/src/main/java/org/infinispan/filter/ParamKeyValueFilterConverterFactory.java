package org.infinispan.filter;

/**
 * Factory for {@link org.infinispan.filter.KeyValueFilterConverter} instances supporting
 * parameters.
 *
 * @author gustavonalle
 * @since 8.1
 */
public interface ParamKeyValueFilterConverterFactory<K, V, C> extends KeyValueFilterConverterFactory<K, V, C> {

   /**
    * Create an instance of {@link KeyValueFilterConverter}
    * @param params Supplied params
    * @return KeyValueFilterConverter
    */
   KeyValueFilterConverter<K, V, C> getFilterConverter(Object[] params);

   /**
    * @return true if parameters should be passed in binary format to the filter.
    */
   default boolean binaryParam() {
      return false;
   }

   @Override
   default KeyValueFilterConverter<K, V, C> getFilterConverter() {
      return getFilterConverter(null);
   }
}
