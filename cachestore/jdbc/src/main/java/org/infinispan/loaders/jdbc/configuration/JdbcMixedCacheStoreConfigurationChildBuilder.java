package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfigurationBuilder.MixedTableManipulationConfigurationBuilder;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;

public interface JdbcMixedCacheStoreConfigurationChildBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>> extends JdbcCacheStoreConfigurationChildBuilder<S> {

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with binary keys
    */
   MixedTableManipulationConfigurationBuilder binaryTable();

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with string keys
    */
   MixedTableManipulationConfigurationBuilder stringTable();

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   JdbcMixedCacheStoreConfigurationChildBuilder<S> key2StringMapper(String key2StringMapper);

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   JdbcMixedCacheStoreConfigurationChildBuilder<S> key2StringMapper(Class<? extends Key2StringMapper> klass);

}