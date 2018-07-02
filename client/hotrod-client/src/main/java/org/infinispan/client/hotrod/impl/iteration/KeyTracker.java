package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.commons.configuration.ClassWhiteList;

/**
 * @author gustavonalle
 * @since 8.0
 */
public interface KeyTracker {

   boolean track(byte[] key, short status, ClassWhiteList whitelist);

   void segmentsFinished(byte[] finishedSegments);

   Set<Integer> missedSegments();
}
