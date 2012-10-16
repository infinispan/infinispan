/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.cdi.test.distexec;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.Input;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.distexec.DistributedCallable;
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
@Test(enabled = true, groups = "functional", testName = "distexec.DistributedExecutorTest")
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

   public void testInvocationUsingDistributedCallable() throws Exception {
      delegate.basicInvocation(new DistributedCacheCallable(false));
   }

   public void testInvocationUsingDistributedCallableWithInputCache() throws Exception {
      delegate.basicInvocation(new DistributedCacheCallableWithInputCache());
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvocationException() throws Exception {
      try {
         delegate.basicInvocation(new DistributedCacheCallable(true));
      } catch(ExecutionException ex) {
         ex.printStackTrace();

         Throwable rootCause = ex.getCause();
         String message = null;
         while(rootCause != null) {
            message = rootCause.getMessage();
            rootCause = rootCause.getCause();
         }

         Assert.assertEquals(message, "/ by zero", "The exception should be arithmetic exception.");
         throw ex;
      }
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

   static class DistributedCacheCallable implements DistributedCallable<String, String, Integer>, Serializable {
      @Inject
      private Cache<String, String> cache;
      private boolean throwException = false;

      public DistributedCacheCallable(final boolean throwException) {
         this.throwException = throwException;
      }

      @Override
      public Integer call() throws Exception {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         Assert.assertFalse(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));

         if(throwException) {
            //throwing run time exception for simulating exception part
            int a = 4 / 0;
         }
         return 1;
      }

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         Assert.assertNotSame(this.cache, cache);
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));
      }
   }

   static class DistributedCacheCallableWithInputCache implements DistributedCallable<String, String, Integer>, Serializable {
      @Inject
      @Input
      private Cache<String, String> cache;

      @Override
      public Integer call() throws Exception {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));
         return 1;
      }

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         Assert.assertSame(this.cache, cache);
         Assert.assertTrue(cache.getName().equals("DistributedExecutorTest-DIST_SYNC"));
      }
   }
}
