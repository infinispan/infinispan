package org.infinispan.cdi.embedded.test.distexec;

import static org.infinispan.cdi.embedded.test.testutil.Deployments.baseDeployment;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.embedded.Input;
import org.infinispan.cdi.embedded.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.distexec.DistributedExecutorTest;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.MultipleCacheManagersTest;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests CDI integration with org.infinispan.distexec.DistributedExecutorService
 *
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "cdi.test.distexec.DistributedExecutorCDITest")
public class DistributedExecutorCDITest extends MultipleCacheManagersArquillianTest {

   DistributedExecutorTest delegate;

   public DistributedExecutorCDITest() {
      delegate = new DistributedExecutorTest();
   }

   @Override
   MultipleCacheManagersTest getDelegate() {
      return delegate;
   }

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addClass(DistributedExecutorCDITest.class)
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   public void testBasicInvocation() throws Exception {
      delegate.basicInvocation(new SimpleCallable());
   }

   public void testInvocationUsingImpliedInputCache() throws Exception {
      delegate.basicInvocation(new ImpliedInputCacheCallable());
   }

   public void testInvocationUsingImpliedInputCacheWithKeys() throws Exception {
      Map<Object,Object> entries = new HashMap<>();
      entries.put("test1", "test1");
      entries.put("test2", "test2");
      delegate.addEntries(entries);
      delegate.basicInvocation(new ImpliedInputCacheWithKeysCallable(), "test1", "test2");
   }

   public void testBasicInvocationRunnable() throws Exception {
      delegate.basicInvocation(new SimpleRunnable());
   }

   public void testInvocationUsingImpliedInputCacheRunnable() throws Exception {
      delegate.basicInvocation(new ImpliedInputCacheRunnable());
   }


   static class SimpleCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Inject
      private Cache<String, String> cache;

      @Override
      public Integer call() throws Exception {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         return 1;
      }
   }

   static class SimpleRunnable implements Runnable, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Inject
      private Cache<String, String> cache;

      @Override
      public void run() {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
      }
   }

   static class ImpliedInputCacheCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = 5770069398989111268L;

      @Input
      @Inject
      private Cache<String, String> cache;

      @Override
      public Integer call() throws Exception {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         //verify the right cache injected
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));
         return 1;
      }
   }

   static class ImpliedInputCacheWithKeysCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = 5770069398989111268L;

      @Input
      @Inject
      private Cache<String, String> cache;

      @Input
      @Inject
      private Collection<String> keys;

      @Override
      public Integer call() throws Exception {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         //verify the right cache injected
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));

         Assert.assertNotNull(keys, "Cache not injected into " + this);
         //verify the right number of keys injected
         Assert.assertTrue(keys.size() == 2);
         return 1;
      }
   }

   static class ImpliedInputCacheRunnable implements Runnable, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = 5770069398989111268L;

      @Input
      @Inject
      private Cache<String, String> cache;

      @Override
      public void run() {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         //verify the right cache injected
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));
      }
   }
}
