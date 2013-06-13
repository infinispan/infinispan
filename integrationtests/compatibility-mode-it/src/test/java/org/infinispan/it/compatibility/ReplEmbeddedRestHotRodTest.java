/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.it.compatibility;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

/**
 * Tests embedded, Hot Rod and REST compatibility in a replicated
 * clustered environment.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.ReplEmbeddedHotRodTest")
public class ReplEmbeddedRestHotRodTest {

   CompatibilityCacheFactory<String, Object> cacheFactory1;
   CompatibilityCacheFactory<String, Object> cacheFactory2;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new CompatibilityCacheFactory<String, Object>(CacheMode.REPL_SYNC).setup();
      cacheFactory2 = new CompatibilityCacheFactory<String, Object>(CacheMode.REPL_SYNC)
            .setup(cacheFactory1.getHotRodPort(), 100);
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

   public void testRestPutEmbeddedHotRodGet() throws Exception {
      final String key = "1";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory1.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory1.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory2.getEmbeddedCache().get(key));

      // 3. Get with Hot Rod
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory2.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() throws Exception {
      final String key = "2";

      // 1. Put with Embedded
      assertEquals(null, cacheFactory2.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Hot Rod
      assertEquals("v1", cacheFactory2.getHotRodCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory2.getRestUrl() + "/" + key);
      cacheFactory2.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testHotRodPutEmbeddedRestGet() throws Exception {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory1.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory2.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory2.getRestUrl() + "/" + key);
      cacheFactory2.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

}
