package org.infinispan.xsite.irac;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Verifies a remove replicated from another site notifies listeners with the removed value.
 * <p>
 * Covers entries resident in memory and entries only in the store of the receiving site, with and without passivation.
 * </p>
 */
@Test(groups = "functional", testName = "xsite.irac.IracRemovedEventPreviousValueTest")
public class IracRemovedEventPreviousValueTest extends AbstractMultipleSitesTest {

   private boolean passivation;

   public IracRemovedEventPreviousValueTest withPassivation(boolean passivation) {
      this.passivation = passivation;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new IracRemovedEventPreviousValueTest().withPassivation(true),
            new IracRemovedEventPreviousValueTest().withPassivation(false)
      };
   }

   @Override
   protected String[] parameterNames() {
      return new String[] {"passivation"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] {passivation};
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 1;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      builder.sites().addBackup()
            .site(siteIndex == 0 ? siteName(1) : siteName(0))
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      // Without a store name, each node gets its own store.
      builder.persistence().passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

   public void testRemovedEventCarriesPreviousValueOfResidentEntry() {
      putInAllSites("resident");

      assertThat(removedValuesAtBackupSite("resident")).containsExactly("value");
   }

   public void testRemovedEventCarriesPreviousValueOfStoredEntry() {
      putInAllSites("stored");
      // Leaves the entry only in the store of the site receiving the remove.
      this.<String, String>cache(1, 0).evict("stored");

      assertThat(removedValuesAtBackupSite("stored")).containsExactly("value");
   }

   private void putInAllSites(String key) {
      this.<String, String>cache(0, 0).put(key, "value");
      this.<String, String>eventuallyAssertInAllSitesAndCaches(c -> Objects.equals("value", c.get(key)));
   }

   /**
    * Removes {@code key} in the first site and returns the previous values the second site's listener observed after
    * the replicated remove committed there.
    */
   private List<String> removedValuesAtBackupSite(String key) {
      Cache<String, String> backupSiteCache = cache(1, 0);
      RemovedListener listener = new RemovedListener();
      backupSiteCache.addListener(listener);
      try {
         this.<String, String>cache(0, 0).remove(key);
         eventually(() -> !listener.previousValues.isEmpty());
      } finally {
         backupSiteCache.removeListener(listener);
      }
      return listener.previousValues;
   }

   @Listener(observation = Listener.Observation.POST)
   public static class RemovedListener {
      final List<String> previousValues = new CopyOnWriteArrayList<>();

      @CacheEntryRemoved
      public void onRemoved(CacheEntryRemovedEvent<String, String> event) {
         previousValues.add(event.getOldValue());
      }
   }
}
