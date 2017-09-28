package org.infinispan.counter.impl.listener;

/**
 * The listener to be invoked when the cache topology changes.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@FunctionalInterface
public interface TopologyChangeListener {

   /**
    * It notifies the cache topology change.
    */
   void topologyChanged();

}
