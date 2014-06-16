package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * ExecutionAuthorizationTest.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "security.ExecutionAuthorizationTest")
public class ExecutionAuthorizationTest extends MultipleCacheManagersTest {
   private static final String EXECUTION_CACHE = "executioncache";
   Subject ADMIN = TestingUtil.makeSubject("admin");
   Subject EXEC = TestingUtil.makeSubject("exec");
   Subject NOEXEC = TestingUtil.makeSubject("noexec");



   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.security().authorization().enable().role("admin").role("exec").role("noexec");
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            addClusterEnabledCacheManager(getSecureClusteredGlobalConfiguration(), builder);
            addClusterEnabledCacheManager(getSecureClusteredGlobalConfiguration(), builder);
            for (EmbeddedCacheManager cm : cacheManagers) {
               cm.defineConfiguration(EXECUTION_CACHE, builder.build());
               cm.getCache(EXECUTION_CACHE);
            }
            waitForClusterToForm(EXECUTION_CACHE);
            return null;
         }
      });
   }

   private GlobalConfigurationBuilder getSecureClusteredGlobalConfiguration() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.security().authorization()
         .enable()
         .principalRoleMapper(new IdentityRoleMapper())
         .role("admin")
            .permission(AuthorizationPermission.ALL)
         .role("exec")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE)
            .permission(AuthorizationPermission.EXEC)
         .role("noexec")
            .permission(AuthorizationPermission.READ)
            .permission(AuthorizationPermission.WRITE);
      return global;
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            ExecutionAuthorizationTest.super.destroy();
            return null;
         }
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void clearContent() throws Exception {
      Subject.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            try {
               ExecutionAuthorizationTest.super.clearContent();
            } catch (Throwable e) {
               throw new Exception(e);
            }
            return null;
         }

      });
   }

   private void distExecTest() throws Exception {
      DefaultExecutorService des = new DefaultExecutorService(cache(0, EXECUTION_CACHE));
      NotifyingFuture<Integer> future = des.submit(new SimpleCallable());
      assertEquals(Integer.valueOf(1), future.get());
   }

   public void testExecDistExec() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         Subject.doAs(EXEC, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               distExecTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testNoExecDistExec() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      try {
         System.setSecurityManager(new SecurityManager());
         Subject.doAs(NOEXEC, new PrivilegedExceptionAction<Void>() {

            @Override
            public Void run() throws Exception {
               distExecTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   private void mapReduceTest() {
      Cache c1 = cache(0, EXECUTION_CACHE);
      Cache c2 = cache(1, EXECUTION_CACHE);

      c1.put("1", "Hello world here I am");
      c2.put("2", "Infinispan rules the world");
      c1.put("3", "JUDCon is in Boston");
      c2.put("4", "JBoss World is in Boston as well");
      c1.put("12","JBoss Application Server");
      c2.put("15", "Hello world");
      c1.put("14", "Infinispan community");

      c1.put("111", "Infinispan open source");
      c2.put("112", "Boston is close to Toronto");
      c1.put("113", "Toronto is a capital of Ontario");
      c2.put("114", "JUDCon is cool");
      c1.put("211", "JBoss World is awesome");
      c2.put("212", "JBoss rules");
      c1.put("213", "JBoss division of RedHat ");
      c2.put("214", "RedHat community");

      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(c1);
      task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
      task.execute();
   }

   public void testExecMapReduce() {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         Subject.doAs(EXEC, new PrivilegedAction<Void>() {

            @Override
            public Void run() {
               mapReduceTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   @Test(expectedExceptions=SecurityException.class)
   public void testNoExecMapReduce() {
      Policy.setPolicy(new SurefireTestingPolicy());
      try {
         System.setSecurityManager(new SecurityManager());
         Subject.doAs(NOEXEC, new PrivilegedAction<Void>() {

            @Override
            public Void run() {
               mapReduceTest();
               return null;
            }
         });
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      public SimpleCallable() {
      }

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }
   static class WordCountMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         if(value == null) throw new IllegalArgumentException("Key " + key + " has value " + value);
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            collector.emit(s, 1);
         }
      }
   }

   static class WordCountReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         int sum = 0;
         while (iter.hasNext()) {
            sum += iter.next();
         }
         return sum;
      }
   }

}
