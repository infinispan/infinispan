package org.infinispan.distribution.ch;

public interface AffinityTaggedKey {

   /**
    * This numeric id is used exclusively for storage affinity in Infinispan.
    *
    * @return the segment id to be used for storage, or -1 to fall back to normal owner selection.
    */
   int getAffinitySegmentId();

}
