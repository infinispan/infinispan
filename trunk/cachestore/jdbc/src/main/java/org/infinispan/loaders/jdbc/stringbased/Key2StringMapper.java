package org.infinispan.loaders.jdbc.stringbased;

/**
 * Defines the logic of mapping an key object to a String. This is required {@link JdbcStringBasedCacheStore}, in order
 * to map each key as an single row within a database. It basically should generate a unique String PK based on the
 * supplied key.
 *
 * @author Mircea.Markus@jboss.com
 */
public interface Key2StringMapper {

   /**
    * Do we support this key type?
    */
   boolean isSupportedType(Class keyType);

   /**
    * Must return an unique String for the supplied key.
    */
   String getStringMapping(Object key);
}
