package org.infinispan.distribution.ch.impl;

import org.infinispan.distribution.ch.AffinityTaggedKey;

/**
 * Key partitioner that maps keys to segments using information contained in {@link AffinityTaggedKey}.
 * <p>If the segment is not defined (value -1) or the key is not an AffinityTaggedKey, will fallback to a {@link HashFunctionPartitioner}
 *
 * @author gustavonalle
 * @since 8.2
 */
public class AffinityPartitioner extends HashFunctionPartitioner {

   @Override
   public int getSegment(Object key) {
      if (key instanceof AffinityTaggedKey) {
         int affinitySegmentId = ((AffinityTaggedKey) key).getAffinitySegmentId();
         if (affinitySegmentId != -1) {
            return affinitySegmentId;
         }
      }
      return super.getSegment(key);
   }

}
