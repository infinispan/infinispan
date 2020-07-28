package org.infinispan.xsite.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A {@link XSiteEntryMergePolicy} implementation that chooses a non-null entry.
 * <p>
 * If both entries are null (or non-null), then it uses the {@link DefaultXSiteEntryMergePolicy} to resolve the conflict.
 *
 * @author Pedro Ruivo
 * @see DefaultXSiteEntryMergePolicy
 * @since 12.0
 */
public class PreferNonNullXSiteEntryMergePolicy<K, V> implements XSiteEntryMergePolicy<K, V> {

   private static final PreferNonNullXSiteEntryMergePolicy<?, ?> INSTANCE = new PreferNonNullXSiteEntryMergePolicy<>();

   private PreferNonNullXSiteEntryMergePolicy() {
   }

   public static <T, U> PreferNonNullXSiteEntryMergePolicy<T, U> getInstance() {
      //noinspection unchecked
      return (PreferNonNullXSiteEntryMergePolicy<T, U>) INSTANCE;
   }

   @Override
   public CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry) {
      boolean localIsNull = localEntry.getValue() == null;
      boolean remoteIsNull = remoteEntry.getValue() == null;
      if (localIsNull == remoteIsNull) { //both are null or bot are non-null
         return DefaultXSiteEntryMergePolicy.<K, V>getInstance().merge(key, localEntry, remoteEntry);
      } else if (localIsNull) {
         return CompletableFuture.completedFuture(remoteEntry);
      } else {
         return CompletableFuture.completedFuture(localEntry);
      }
   }
}
