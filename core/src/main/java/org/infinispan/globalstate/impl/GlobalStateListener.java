package org.infinispan.globalstate.impl;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;

/**
 * // TODO: Document this
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
@Listener
public class GlobalStateListener {

   @Merged
   public void clusterMerged(MergeEvent event) {

   }
}
