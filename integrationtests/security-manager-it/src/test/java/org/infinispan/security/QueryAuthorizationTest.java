package org.infinispan.security;

import static junit.framework.Assert.assertEquals;

import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.api.TestEntity;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * QueryAuthorizationTest.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "security.QueryAuthorizationTest")
public class QueryAuthorizationTest extends SingleCacheManagerTest {
   Subject ADMIN = TestingUtil.makeSubject("admin");
   Subject QUERY = TestingUtil.makeSubject("query");
   Subject NOQUERY = TestingUtil.makeSubject("noquery");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder
         .indexing().index(Index.LOCAL).addProperty("default.directory_provider", "ram").addProperty("lucene_version", "LUCENE_CURRENT")
         .security()
            .authorization().enable().role("admin").role("query").role("noquery");
      return Subject.doAs(ADMIN, new PrivilegedAction<EmbeddedCacheManager>() {

         @Override
         public EmbeddedCacheManager run() {
            EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(getSecureGlobalConfiguration(), builder);
            ecm.getCache();
            return ecm;
         }
      });
   }

   private GlobalConfigurationBuilder getSecureGlobalConfiguration() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.security().authorization()
         .enable()
         .principalRoleMapper(new IdentityRoleMapper())
         .role("admin")
            .permission(AuthorizationPermission.ALL)
         .role("query")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.BULK_READ)
         .role("noquery")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE);
      return global;
   }

   @Override
   protected void teardown() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            QueryAuthorizationTest.super.teardown();
            return null;
         }
      });
   }

   @Override
   protected void clearContent() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            cacheManager.getCache().clear();
            return null;
         }
      });
   }

   private void queryTest() {
      cache.put("jekyll", new TestEntity("Henry", "Jekyll", 1, "dissociate identity disorder"));
      cache.put("hyde", new TestEntity("Edward", "Hyde", 2, "dissociate identity disorder"));
      SearchManager sm = Search.getSearchManager(cache);
      Query query = sm.buildQueryBuilderForClass(TestEntity.class)
            .get().keyword().onField("name").matching("Henry").createQuery();
      CacheQuery q = sm.getQuery(query);
      assertEquals(1, q.getResultSize());
      assertEquals(TestEntity.class, q.list().get(0).getClass());
   }

   public void testQuery() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         Subject.doAs(QUERY, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               queryTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testNoQuery() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      try {
         System.setSecurityManager(new SecurityManager());
         Subject.doAs(NOQUERY, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               queryTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }
}
