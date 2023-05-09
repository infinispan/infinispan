package org.infinispan.xsite;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.xsite.spi.SiteEntry;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;

/**
 * @author Pedro Ruivo
 * @since 12.0
 */
public class CustomXSiteEntryMergePolicy<K, V> implements XSiteEntryMergePolicy<K, V> {

   @Override
   public CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry) {
      return null; //just for config test. not really used.
   }

   @Override
   public boolean equals(Object obj) {
      return obj != null && Objects.equals(obj.getClass(), getClass());
   }
}
