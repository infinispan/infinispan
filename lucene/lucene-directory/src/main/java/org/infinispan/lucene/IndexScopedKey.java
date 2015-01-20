package org.infinispan.lucene;

import org.infinispan.distribution.ch.AffinityTaggedKey;

/**
 * Mostly used for internal abstraction: common type for all keys which need name scoping for different indexes.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
public interface IndexScopedKey extends AffinityTaggedKey {

   String getIndexName();

   <T> T accept(KeyVisitor<T> visitor) throws Exception;

}
