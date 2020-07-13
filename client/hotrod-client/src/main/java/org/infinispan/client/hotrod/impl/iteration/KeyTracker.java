package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.IntSet;

/**
 * @author gustavonalle
 * @since 8.0
 */
public interface KeyTracker {

   boolean track(byte[] key, short status, ClassAllowList allowList);

   void segmentsFinished(IntSet finishedSegments);

   Set<Integer> missedSegments();
}
