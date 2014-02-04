package org.infinispan.cdi.test.distexec;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;

import java.io.Serializable;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.Input;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.distexec.DistributedExecutorTest;
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
   

   static class SimpleCallable implements Callable<Integer>, Serializable {

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
   
   static class ImpliedInputCacheCallable implements Callable<Integer>, Serializable {

   
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
}
