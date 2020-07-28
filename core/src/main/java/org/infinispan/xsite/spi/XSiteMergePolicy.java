package org.infinispan.xsite.spi;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.SitesConfigurationBuilder;

/**
 * An alias with the provided implementation of {@link XSiteEntryMergePolicy}.
 * <p>
 * To be used in {@link SitesConfigurationBuilder#mergePolicy(XSiteEntryMergePolicy)}
 *
 * @author Pedro Ruivo
 * @see XSiteEntryMergePolicy
 * @see PreferNonNullXSiteEntryMergePolicy
 * @see PreferNullXSiteEntryMergePolicy
 * @see AlwaysRemoveXSiteEntryMergePolicy
 * @see DefaultXSiteEntryMergePolicy
 * @since 12.0
 */
public enum XSiteMergePolicy implements XSiteEntryMergePolicy<Object, Object> {
   /**
    * Chooses the {@code non-null} value if available (write/remove conflict, write wins), otherwise uses the {@link
    * #DEFAULT}.
    *
    * @see PreferNonNullXSiteEntryMergePolicy
    */
   PREFER_NON_NULL {
      @Override
      public <K, V> XSiteEntryMergePolicy<K, V> getInstance() {
         return PreferNonNullXSiteEntryMergePolicy.getInstance();
      }
   },
   /**
    * Chooses the {@code null} value if available (write/remove conflict, remove wins), otherwise uses the {@link
    * #DEFAULT}.
    *
    * @see PreferNullXSiteEntryMergePolicy
    */
   PREFER_NULL {
      @Override
      public <K, V> XSiteEntryMergePolicy<K, V> getInstance() {
         return PreferNullXSiteEntryMergePolicy.getInstance();
      }
   },
   /**
    * Always remove the key if there is a conflict.
    *
    * @see AlwaysRemoveXSiteEntryMergePolicy
    */
   ALWAYS_REMOVE {
      @Override
      public <K, V> XSiteEntryMergePolicy<K, V> getInstance() {
         return AlwaysRemoveXSiteEntryMergePolicy.getInstance();
      }
   },
   /**
    * The default implementation chooses the entry with the lower lexicographically site name ({@link
    * SiteEntry#getSiteName()}).
    *
    * @see DefaultXSiteEntryMergePolicy
    */
   DEFAULT {
      @Override
      public <K, V> XSiteEntryMergePolicy<K, V> getInstance() {
         return DefaultXSiteEntryMergePolicy.getInstance();
      }
   };


   public static XSiteMergePolicy fromString(String str) {
      for (XSiteMergePolicy mergePolicy : XSiteMergePolicy.values()) {
         if (mergePolicy.name().equalsIgnoreCase(str)) {
            return mergePolicy;
         }
      }
      return null;
   }

   public static <K, V> XSiteMergePolicy fromInstance(XSiteEntryMergePolicy<K, V> r2) {
      for (XSiteMergePolicy mergePolicy : XSiteMergePolicy.values()) {
         XSiteEntryMergePolicy<K, V> r1 = mergePolicy.getInstance();
         if (Objects.equals(r1, r2)) {
            return mergePolicy;
         }
      }
      return null;
   }

   public static <T, U> XSiteEntryMergePolicy<T, U> instanceFromString(String value, ClassLoader classLoader) {
      XSiteMergePolicy mergePolicy = XSiteMergePolicy.fromString(value);
      return mergePolicy == null ? Util.getInstance(value, classLoader) : mergePolicy.getInstance();
   }

   @Override
   public CompletionStage<SiteEntry<Object>> merge(Object key, SiteEntry<Object> localEntry, SiteEntry<Object> remoteEntry) {
      throw new UnsupportedOperationException();
   }

   public abstract <K, V> XSiteEntryMergePolicy<K, V> getInstance();
}
