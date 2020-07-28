package org.infinispan.xsite.spi;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * An interface to resolve conflicts for asynchronous cross-site replication.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public interface XSiteEntryMergePolicy<K, V> {

   /**
    * Resolves conflicts for asynchronous cross-site replication.
    * <p>
    * When a conflict is detected (concurrent updates on the same key in different sites), this method is invoked with
    * the local data and the remote site's data ({@link SiteEntry}). It includes the value and the {@link Metadata}
    * associated.
    * <p>
    * The value and the {@link Metadata} may be {@code null}. If that is the case, it means the {@code key} doesn't
    * exist (for {@code localEntry}) or it is a remove operation (for {@code remoteEntry}).
    * <p>
    * The returned {@link SiteEntry} must be equal independent of the order of the arguments (i.e. {@code resolve(k, s1,
    * s2).equals(resolve(k, s2, s1))}) otherwise your date may be corrupted. It is allowed to return one of the
    * arguments ({@code localEntry} or {@code remoteEntry}) and to create a new {@link SiteEntry} with a new value.
    * <p>
    * Note: if the return {@link SiteEntry#getValue()} is {@code null}, Infinispan will interpret it to remove the
    * {@code key}.
    * <p>
    * Note2: This method shouldn't block (I/O or locks). If it needs to block, use a different thread and complete the
    * {@link CompletionStage} with the result. We recommend using {@link BlockingManager#supplyBlocking(Supplier,
    * Object)}.
    *
    * @param key         The key that was updated concurrently.
    * @param localEntry  The local value and {@link Metadata} stored.
    * @param remoteEntry The remote value and {@link Metadata} received.
    * @return A {@link CompletionStage} with the {@link SiteEntry}.
    */
   CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry);
}
