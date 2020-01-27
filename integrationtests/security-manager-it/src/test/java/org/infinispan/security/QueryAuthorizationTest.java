package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.api.TestEntity;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.security.mappers.IdentityRoleMapper;
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

   private Subject ADMIN = TestingUtil.makeSubject("admin");

   private Subject QUERY = TestingUtil.makeSubject("query");

   private Subject NOQUERY = TestingUtil.makeSubject("noquery");

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      final ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder
         .indexing()
            .enable()
            .addIndexedEntity(TestEntity.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT")
         .security()
            .authorization().enable().role("admin").role("query").role("noquery");

      return Subject.doAs(ADMIN, (PrivilegedAction<EmbeddedCacheManager>) () -> {
         EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(getSecureGlobalConfiguration(), builder);
         ecm.getCache();
         return ecm;
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
      Subject.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         QueryAuthorizationTest.super.teardown();
         return null;
      });
   }

   @Override
   protected void clearContent() {
      Subject.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         cacheManager.getCache().clear();
         return null;
      });
   }

   private void queryTest() {
      cache.put("jekyll", new TestEntity("Henry", "Jekyll", 1, "dissociate identity disorder"));
      cache.put("hyde", new TestEntity("Edward", "Hyde", 2, "dissociate identity disorder"));
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query q = queryFactory.create(String.format("FROM %s where name = 'Henry'", TestEntity.class.getName()));
      assertEquals(1, q.getResultSize());
      assertEquals(TestEntity.class, q.list().get(0).getClass());
   }

   public void testQuery() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         Subject.doAs(QUERY, (PrivilegedExceptionAction<Void>) () -> {
            queryTest();
            return null;
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
         Subject.doAs(NOQUERY, (PrivilegedExceptionAction<Void>) () -> {
            queryTest();
            return null;
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }
}
