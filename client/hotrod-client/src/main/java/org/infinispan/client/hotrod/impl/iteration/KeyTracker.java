package org.infinispan.client.hotrod.impl.iteration;

import java.util.List;
import java.util.Set;

/**
 * @author gustavonalle
 * @since 8.0
 */
public interface KeyTracker {

   boolean track(byte[] key, short status, List<String> whitelist);

   void segmentsFinished(byte[] finishedSegments);

   Set<Integer> missedSegments();
}
