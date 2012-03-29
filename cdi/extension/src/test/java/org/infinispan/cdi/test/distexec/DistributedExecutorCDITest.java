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
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.infinispan.Cache;
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
      return baseDeployment().addClass(DistributedExecutorCDITest.class);
   }
   
   public void testBasicInvocation() throws Exception {
      delegate.basicInvocation(new SimpleCallable());
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
}
