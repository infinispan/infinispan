package org.infinispan.xsite.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The default {@link XSiteEntryMergePolicy} implementation.
 * <p>
 * It uses the site's name to deterministically choose the winning entry. The site with the name lexicographically
 * lowers wins.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class DefaultXSiteEntryMergePolicy<K, V> implements XSiteEntryMergePolicy<K, V> {

   private static final DefaultXSiteEntryMergePolicy<?, ?> INSTANCE = new DefaultXSiteEntryMergePolicy<>();

   private DefaultXSiteEntryMergePolicy() {
   }

   public static <U, W> DefaultXSiteEntryMergePolicy<U, W> getInstance() {
      //noinspection unchecked
      return (DefaultXSiteEntryMergePolicy<U, W>) INSTANCE;
   }

   @Override
   public CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry) {
      assert !localEntry.getSiteName().equals(remoteEntry.getSiteName());
      return CompletableFuture.completedFuture(localEntry.getSiteName().compareTo(remoteEntry.getSiteName()) <= 0 ?
            localEntry :
            remoteEntry);
   }
}
