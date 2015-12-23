package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.distribution.ch.AffinityTaggedKey;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

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

   public static class Externalizer extends NoStateExternalizer<AffinityPartitioner> {

      @Override
      public Set<Class<? extends AffinityPartitioner>> getTypeClasses() {
         return Collections.singleton(AffinityPartitioner.class);
      }

      @Override
      public AffinityPartitioner readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new AffinityPartitioner();
      }

      @Override
      public Integer getId() {
         return Ids.AFFINITY_FUNCTION_PARTITIONER;
      }
   }
}
