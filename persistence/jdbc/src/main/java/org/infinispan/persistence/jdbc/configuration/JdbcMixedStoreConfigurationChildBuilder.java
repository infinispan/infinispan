package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;

public interface JdbcMixedStoreConfigurationChildBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends JdbcStoreConfigurationChildBuilder<S> {

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with binary keys
    */
   JdbcMixedStoreConfigurationBuilder.MixedTableManipulationConfigurationBuilder binaryTable();

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with string keys
    */
   JdbcMixedStoreConfigurationBuilder.MixedTableManipulationConfigurationBuilder stringTable();

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   JdbcMixedStoreConfigurationChildBuilder<S> key2StringMapper(String key2StringMapper);

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   JdbcMixedStoreConfigurationChildBuilder<S> key2StringMapper(Class<? extends Key2StringMapper> klass);

}