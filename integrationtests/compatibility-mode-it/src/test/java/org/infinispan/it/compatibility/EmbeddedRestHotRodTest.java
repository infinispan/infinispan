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
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.api.BasicCacheContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import static org.testng.AssertJUnit.*;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "it.compatibility.EmbeddedRestHotRodTest")
public class EmbeddedRestHotRodTest {

   static String REST_ROOT_PATH =
         "http://localhost:8989/rest/" + BasicCacheContainer.DEFAULT_CACHE_NAME;

   CompatibilityCacheFactory<String, byte[]> cacheFactory;

   @BeforeClass(alwaysRun = true)
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, byte[]>();
      cacheFactory.setup();
   }

   @AfterClass(alwaysRun = true)
   protected void teardown() {
      cacheFactory.teardown();
   }

   public void testRestPutEmbeddedHotRodGet() throws Exception {
      EntityEnclosingMethod put = new PutMethod(REST_ROOT_PATH + "/1");
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals("", put.getResponseBodyAsString().trim());
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());

      assertArrayEquals("<hey>ho</hey>".getBytes(), cacheFactory.getEmbeddedCache().get("1"));
      assertArrayEquals("<hey>ho</hey>".getBytes(), cacheFactory.getHotRodCache().get("1"));
   }

}
