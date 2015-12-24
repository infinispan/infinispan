package org.infinispan.lucene;

import org.infinispan.distribution.ch.AffinityTaggedKey;

/**
 * Mostly used for internal abstraction: common type for all keys which need name scoping for different indexes.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
public interface IndexScopedKey extends AffinityTaggedKey {

   /**
    * Different indexes are required to use different names
    * @return
    */
   String getIndexName();

   /**
    * This numeric id is used exclusively for storage affinity in Infinispan.
    * It is not included in the equals and hashcode implementations!
    * @return the segment id as defined in {@link org.infinispan.lucene.directory.BuildContext#affinityLocationIntoSegment(int)}, or -1 when not explicitly set.
    */
   int getAffinitySegmentId();

   <T> T accept(KeyVisitor<T> visitor) throws Exception;

}
