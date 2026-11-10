package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.syntax.update.QueryFunctionRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Asserts that a user-defined query function registered on one embedded cache
 * manager is usable on that manager's caches and rejected at parse time on
 * any other cache manager in the same JVM.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "query.core.tests.QueryFunctionRegistryScopingTest")
public class QueryFunctionRegistryScopingTest extends MultipleCacheManagersTest {

   /**
    * Plain (reflection-based) entity, mirroring the test entities of
    * {@link UpdateQueryCoreTest}.
    */
   @Indexed
   public static class Event {
      public String title;
      public int count;

      public Event() {
      }

      public Event(String title) {
         this.title = title;
      }
   }

   private static final String ENTITY = Event.class.getName();

   private Cache<Integer, Event> cache1;
   private Cache<Integer, Event> cache2;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder1 = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder1.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(ENTITY);
      ConfigurationBuilder builder2 = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder2.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(ENTITY);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
      registerCacheManager(cm1, cm2);

      cache1 = cm1.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("scoping-1", builder1.build());
      cache2 = cm2.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("scoping-2", builder2.build());
   }

   @BeforeMethod
   public void populateCache() {
      cache1.clear();
      cache1.put(1, new Event("Alpha"));
      cache1.put(2, new Event("Beta"));
      cache2.clear();
      cache2.put(1, new Event("Alpha"));
   }

   @Test
   public void testUdfIsScopedToItsCacheManager() {
      QueryFunctionRegistry registry1 = GlobalComponentRegistry.componentOf(manager(0), QueryFunctionRegistry.class);
      QueryFunctionRegistry registry2 = GlobalComponentRegistry.componentOf(manager(1), QueryFunctionRegistry.class);
      assertThat(registry1).isNotNull();
      assertThat(registry2).isNotNull();
      assertThat(registry1).isNotSameAs(registry2);

      registry1.add("tag", new TagFunction());
      assertThat(registry1.contains("tag")).isTrue();
      assertThat(registry2.contains("tag")).isFalse();

      // The UDF is usable on a cache of the registering cache manager.
      Query<Event> update = cache1.query(
            "UPDATE FROM " + ENTITY + " SET title = tag(title) WHERE title = 'Alpha'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(cache1.get(1).title).isEqualTo("tag:Alpha");
      assertThat(cache1.get(2).title).isEqualTo("Beta");

      // The same statement is rejected at parse time on the other cache
      // manager, whose registry does not know the function.
      assertThatThrownBy(() -> cache2.query(
            "UPDATE FROM " + ENTITY + " SET title = tag(title) WHERE title = 'Alpha'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028535");
   }

   /**
    * Trivial user-defined function: prefixes its single string argument
    * with "tag:".
    */
   public static class TagFunction implements QueryFunction {
      @QueryFunctionSignature(args = String.class, returns = String.class, category = "string")
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         if (args.size() != 1) {
            throw new IllegalArgumentException("Function tag expects 1 argument but got " + args.size());
         }
         Object s = args.get(0);
         return s == null ? null : "tag:" + s;
      }
   }
}
