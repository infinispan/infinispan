package org.infinispan.xsite.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A {@link XSiteEntryMergePolicy} that removes the key if a conflict is detected.
 *
 * @author Pedro Ruivo
 * @see XSiteEntryMergePolicy
 * @since 12.0
 */
public class AlwaysRemoveXSiteEntryMergePolicy<K, V> implements XSiteEntryMergePolicy<K, V> {

   private static final AlwaysRemoveXSiteEntryMergePolicy<?, ?> INSTANCE = new AlwaysRemoveXSiteEntryMergePolicy<>();

   private AlwaysRemoveXSiteEntryMergePolicy() {
   }

   public static <T, U> AlwaysRemoveXSiteEntryMergePolicy<T, U> getInstance() {
      //noinspection unchecked
      return (AlwaysRemoveXSiteEntryMergePolicy<T, U>) INSTANCE;
   }

   private static String computeSiteName(SiteEntry<?> entry1, SiteEntry<?> entry2) {
      return entry1.getSiteName().compareTo(entry2.getSiteName()) < 0 ?
             entry1.getSiteName() :
             entry2.getSiteName();
   }

   @Override
   public CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry) {
      SiteEntry<V> resolved = new SiteEntry<>(computeSiteName(localEntry, remoteEntry), null, null);
      return CompletableFuture.completedFuture(resolved);
   }
}
