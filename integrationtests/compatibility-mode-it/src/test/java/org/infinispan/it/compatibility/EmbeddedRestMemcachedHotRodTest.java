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

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.io.ByteBuffer;
import org.infinispan.marshall.AbstractMarshaller;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test compatibility between embedded caches, Hot Rod, REST and Memcached endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.EmbeddedRestMemcachedHotRodTest")
public class EmbeddedRestMemcachedHotRodTest {

   // Memcached cache name cannot be changed right now
   final static String CACHE_NAME = "memcachedCache";

   CompatibilityCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, Object>(
            CACHE_NAME, new SpyMemcachedCompatibleMarshaller());
      cacheFactory.setup();
   }

   @AfterClass
   protected void teardown() {
      cacheFactory.teardown();
   }

   public void testMemcachedPutEmbeddedRestHotRodGetTest() throws Exception {
      final String key = "1";

      // 1. Put with Memcached
      Future<Boolean> f = cacheFactory.getMemcachedClient().set(key, 0, "v1");
      assertTrue(f.get(60, TimeUnit.SECONDS));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("text/plain", get.getResponseHeader("Content-Type").getValue());
      assertEquals("v1", get.getResponseBodyAsString());

      // 4. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));
   }

   public void testEmbeddedPutMemcachedRestHotRodGetTest() throws Exception {
      final String key = "2";

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Memcached
      assertEquals("v1", cacheFactory.getMemcachedClient().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());

      // 4. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));
   }

   public void testRestPutEmbeddedMemcachedHotRodGetTest() throws Exception {
      final String key = "3";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Memcached (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getMemcachedClient().get(key));

      // 4. Get with Hot Rod (given a marshaller, it can unmarshall the result)
      assertEquals("<hey>ho</hey>",
            cacheFactory.getHotRodCache().get(key));
   }

   public void testHotRodPutEmbeddedMemcachedRestGetTest() throws Exception {
      final String key = "4";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Memcached
      assertEquals("v1", cacheFactory.getMemcachedClient().get(key));

      // 4. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   private class SpyMemcachedCompatibleMarshaller extends AbstractMarshaller {

      private final Transcoder<Object> transcoder = new SerializingTranscoder();

      @Override
      protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
         CachedData encoded = transcoder.encode(o);
         return new ByteBuffer(encoded.getData(), 0, encoded.getData().length);
      }

      @Override
      public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
         return transcoder.decode(new CachedData(0, buf, length));
      }

      @Override
      public boolean isMarshallable(Object o) throws Exception {
         try {
            transcoder.encode(o);
            return true;
         } catch (Throwable t) {
            return false;
         }
      }
   }

}
