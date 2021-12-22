package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Hibernate;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cfg.Environment;
import org.hibernate.stat.CacheRegionStatistics;
import org.hibernate.stat.Statistics;
import org.hibernate.testing.TestForIssue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

/**
 * @author Gail Badner
 */
public class EntitiesAndCollectionsInSameRegionTest extends SingleNodeTest {

   private static final String REGION_NAME = "ARegion";

   private final AnEntity anEntity;
   private final AnotherEntity anotherEntity;

   public EntitiesAndCollectionsInSameRegionTest() {
      anEntity = new AnEntity();
      anEntity.id = 1;
      anEntity.valuesSet.add("abc");

      anotherEntity = new AnotherEntity();
      anotherEntity.id = 1;
      anotherEntity.valuesSet.add(123);
   }

   @Override
   public List<Object[]> getParameters() {
      return getParameters(true, false, false, false, false);
   }

   @Override
   public Class[] getAnnotatedClasses() {
      return new Class[]{
         AnEntity.class,
         AnotherEntity.class
      };
   }

   @Override
   @SuppressWarnings("unchecked")
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(Environment.CACHE_KEYS_FACTORY, DefaultCacheKeysFactory.SHORT_NAME);

   }

   @Before
   public void setup() throws Exception {

      final Statistics stats = sessionFactory().getStatistics();
      stats.clear();

      withTxSession(
         s -> {
            s.persist(anEntity);
            s.persist(anotherEntity);
         }
      );

      // Then entities should have been cached, but not their collections.
      CacheRegionStatistics cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
      assertEquals(0, cacheStatistics.getMissCount());
      assertEquals(0, cacheStatistics.getHitCount());
      assertEquals(2, cacheStatistics.getPutCount());

      stats.clear();
   }

   @After
   public void cleanup() throws Exception {
      withTxSession(
         s -> {
            s.delete(s.get(AnEntity.class, 1));
            s.delete(s.get(AnotherEntity.class, 1));
         }
      );
   }


   @Test
   @TestForIssue(jiraKey = "HHH-10418")
   public void testEntitiesAndCollections() throws Exception {

      final Statistics stats = sessionFactory().getStatistics();
      stats.clear();

      withTxSession(
         s -> {
            AnEntity anEntity1 = s.get(AnEntity.class, anEntity.id);

            CacheRegionStatistics cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);

            // anEntity1 was cached when it was persisted
            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            stats.clear();

            assertFalse(Hibernate.isInitialized(anEntity1.valuesSet));
            Hibernate.initialize(anEntity1.valuesSet);

            // anEntity1.values gets cached when it gets loadead
            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(1, cacheStatistics.getMissCount());
            assertEquals(0, cacheStatistics.getHitCount());
            assertEquals(1, cacheStatistics.getPutCount());

            stats.clear();

            AnotherEntity anotherEntity1 = s.get(AnotherEntity.class, anotherEntity.id);

            // anotherEntity1 was cached when it was persisted
            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            stats.clear();

            assertFalse(Hibernate.isInitialized(anotherEntity1.valuesSet));
            Hibernate.initialize(anotherEntity1.valuesSet);

            // anotherEntity1.values gets cached when it gets loadead
            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(1, cacheStatistics.getMissCount());
            assertEquals(0, cacheStatistics.getHitCount());
            assertEquals(1, cacheStatistics.getPutCount());
         }
      );

      // The entities and their collections should all be cached now.

      withTxSession(
         s -> {

            stats.clear();

            AnEntity anEntity1 = s.get(AnEntity.class, anEntity.id);

            CacheRegionStatistics cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);

            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            stats.clear();

            assertFalse(Hibernate.isInitialized(anEntity1.valuesSet));
            Hibernate.initialize(anEntity1.valuesSet);

            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            assertEquals(anEntity.valuesSet, anEntity1.valuesSet);

            stats.clear();

            AnotherEntity anotherEntity1 = s.get(AnotherEntity.class, anotherEntity.id);

            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            stats.clear();

            assertFalse(Hibernate.isInitialized(anotherEntity1.valuesSet));
            Hibernate.initialize(anotherEntity1.valuesSet);

            cacheStatistics = stats.getCacheRegionStatistics(REGION_NAME);
            assertEquals(0, cacheStatistics.getMissCount());
            assertEquals(1, cacheStatistics.getHitCount());
            assertEquals(0, cacheStatistics.getPutCount());

            assertEquals(anotherEntity.valuesSet, anotherEntity1.valuesSet);
         }
      );
   }

   @Entity(name = "AnEntity")
   @Cache(usage = CacheConcurrencyStrategy.TRANSACTIONAL, region = REGION_NAME)
   public static class AnEntity {

      @Id
      private int id;

      @ElementCollection
      @Cache(usage = CacheConcurrencyStrategy.TRANSACTIONAL, region = REGION_NAME)
      private Set<String> valuesSet = new HashSet<>();
   }

   @Entity(name = "AnotherEntity")
   @Cache(usage = CacheConcurrencyStrategy.TRANSACTIONAL, region = REGION_NAME)
   public static class AnotherEntity {

      @Id
      private int id;

      @ElementCollection
      @Cache(usage = CacheConcurrencyStrategy.TRANSACTIONAL, region = REGION_NAME)
      private Set<Integer> valuesSet = new HashSet<>();
   }

}
