package org.infinispan.loader.jdbc.stringbased;

/**
 * Defines the logic of mapping an key object to a String. This is required {@link
 * org.infinispan.loader.jdbc.stringbased.JdbcStringBasedCacheStore}, in order to map each {@link
 * org.infinispan.loader.StoredEntry} as an single row within a database. It bassically should generate an unique String PK
 * based on the supplied key.
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
