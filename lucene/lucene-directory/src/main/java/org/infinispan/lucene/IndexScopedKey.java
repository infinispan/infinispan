package org.infinispan.lucene;

/**
 * Mostly used for internal abstraction: common type for all keys which need name scoping for different indexes.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
public interface IndexScopedKey {

   String getIndexName();

   <T> T accept(KeyVisitor<T> visitor) throws Exception;

}
