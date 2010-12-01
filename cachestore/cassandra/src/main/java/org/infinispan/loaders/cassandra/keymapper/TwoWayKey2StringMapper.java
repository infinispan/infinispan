package org.infinispan.loaders.cassandra.keymapper;

/**
 * Extends {@link org.infinispan.loaders.jdbc.stringbased.Key2StringMapper} and allows a bidirectional transformation
 * between keys and strings. This is needed for supporting preload and rehashing during distribution. See {@link
 * org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} for more info on these limitations. Following
 * condition should be satisfied by implementations of this interface:
 * <p/>
 * <b>k1.equals(getKeyMapping(getStringMapping(k1)))</b>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TwoWayKey2StringMapper extends Key2StringMapper {
   Object getKeyMapping(String key);
}
