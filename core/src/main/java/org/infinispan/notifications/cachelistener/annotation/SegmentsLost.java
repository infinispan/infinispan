package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.cachelistener.event.SegmentsLostEvent;

/**
 * This annotation should be used on methods that need to be notified when one or more segments lost every owner,
 * and the data they contained is therefore no longer available in the cluster.
 * <p>
 * This happens when all the owners of a segment leave or crash at the same time, e.g. when a distributed cache
 * configured with {@code numOwners=2} loses two nodes before the data can be rebalanced. The surviving nodes install
 * a new topology in which the affected segments are assigned to new owners, but those owners have no data for them.
 * No {@link CacheEntryRemoved} event is raised for the entries that disappeared, because no remove operation took
 * place, which means listeners maintaining derived state (near caches, search indexes, projections, ...) would
 * otherwise keep stale entries forever.
 * <p>
 * Methods annotated with this annotation should accept a single parameter, a {@link SegmentsLostEvent}, otherwise an
 * {@link IncorrectListenerException} will be thrown when registering the listener.
 * <p>
 * No event is raised when the cache has a shared store, because the new owners simply read the entries back from it;
 * a shared store can be neither passivating nor purged on startup, so its contents always outlive the nodes that
 * wrote them. Private stores do not prevent the event, as they are lost together with the node that owned them. Note
 * that a shared store configured with write-behind may still have lost its most recent writes, which this event does
 * not report.
 * <p>
 * Unlike the entry events, this notification is not replicated: it is raised locally by every node that installs the
 * new topology, including nodes that never owned the lost segments. This means a listener registered with
 * {@code clustered=true} is notified exactly once, by the node it was registered on. The notification is only raised
 * after the new topology has been installed, so it is never a "pre" event
 * ({@link org.infinispan.notifications.cachelistener.event.Event#isPre()} always returns {@code false}).
 * <p>
 * Segments are only reported as lost when the cache remains available. A cache that enters
 * {@link org.infinispan.partitionhandling.AvailabilityMode#DEGRADED_MODE} keeps its consistent hash unchanged, as the
 * data may still come back when the partitions merge, and therefore raises no event; use
 * {@link PartitionStatusChanged} to be notified of those transitions. A cache configured with
 * {@link org.infinispan.partitionhandling.PartitionHandling#ALLOW_READ_WRITES} does update its consistent hash on a
 * split, so each partition reports the segments owned only by the other one: those entries are indeed unreachable
 * from that partition, but note that they may reappear when the partitions merge.
 * <p>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called.
 *
 * @see org.infinispan.notifications.Listener
 * @since 16.4
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to methods.
@Target(ElementType.METHOD)
public @interface SegmentsLost {
}
