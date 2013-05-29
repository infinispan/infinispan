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
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

/**
 * Test compatibility between embedded caches, Hot Rod, and REST endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.EmbeddedRestHotRodTest")
public class EmbeddedRestHotRodTest {

   CompatibilityCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<String, Object>();
      cacheFactory.setup();
   }

   @AfterClass
   protected void teardown() {
      cacheFactory.teardown();
   }

   public void testRestPutEmbeddedHotRodGet() throws Exception {
      final String key = "1";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpServletResponse.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Hot Rod
      assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[])
            cacheFactory.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() throws Exception {
      final String key = "2";

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, "v1"));

      // 2. Get with Hot Rod
      assertEquals("v1", cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testHotRodPutEmbeddedRestGet() throws Exception {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      assertEquals("v1", cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpServletResponse.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testCustomObjectHotRodPutEmbeddedRestGet() throws Exception{
      final String key = "4";
      Person p = new Person("Martin");

      // 1. Put with Hot Rod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with Embedded
      assertEquals(p, cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "application/x-java-serialized-object");
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(get.getStatusText(), HttpServletResponse.SC_OK, get.getStatusCode());
      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(get.getResponseBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutHotRodRestGet() throws Exception{
      final String key = "5";
      Person p = new Person("Galder");

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with Hot Rod
      assertEquals(p, cacheFactory.getHotRodCache().get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "application/x-java-serialized-object");
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(get.getStatusText(), HttpServletResponse.SC_OK, get.getStatusCode());
      // REST finds the Java POJO in-memory and returns the Java serialized version
      assertEquals(p, new ObjectInputStream(get.getResponseBodyAsStream()).readObject());
   }

   public void testCustomObjectEmbeddedPutRestGetAcceptJSONAndXML() throws Exception{
      final String key = "6";
      final Person p = new Person("Anna");

      // 1. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key, p));

      // 2. Get with REST (accept application/json)
      HttpMethod getJson = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getJson.setRequestHeader("Accept", "application/json");
      cacheFactory.getRestClient().executeMethod(getJson);
      assertEquals(getJson.getStatusText(), HttpServletResponse.SC_OK, getJson.getStatusCode());
      assertEquals("{\"name\":\"Anna\"}", getJson.getResponseBodyAsString());

      // 3. Get with REST (accept application/xml)
      HttpMethod getXml = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getXml.setRequestHeader("Accept", "application/xml");
      cacheFactory.getRestClient().executeMethod(getXml);
      assertEquals(getXml.getStatusText(), HttpServletResponse.SC_OK, getXml.getStatusCode());
      assertTrue(getXml.getResponseBodyAsString().contains("<name>Anna</name>"));
   }

   public void testCustomObjectHotRodPutRestGetAcceptJSONAndXML() throws Exception{
      final String key = "7";
      final Person p = new Person("Jakub");

      // 1. Put with HotRod
      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, p));

      // 2. Get with REST (accept application/json)
      HttpMethod getJson = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getJson.setRequestHeader("Accept", "application/json");
      cacheFactory.getRestClient().executeMethod(getJson);
      assertEquals(getJson.getStatusText(), HttpServletResponse.SC_OK, getJson.getStatusCode());
      assertEquals("{\"name\":\"Jakub\"}", getJson.getResponseBodyAsString());

      // 3. Get with REST (accept application/xml)
      HttpMethod getXml = new GetMethod(cacheFactory.getRestUrl() + "/" + key);
      getXml.setRequestHeader("Accept", "application/xml");
      cacheFactory.getRestClient().executeMethod(getXml);
      assertEquals(getXml.getStatusText(), HttpServletResponse.SC_OK, getXml.getStatusCode());
      assertTrue(getXml.getResponseBodyAsString().contains("<name>Jakub</name>"));
   }

   public void testHotRodEmbeddedPutRestHeadExpiry() throws Exception {
      final String key1 = "8";
      final String key2 = "9";

      // 1. Put with HotRod
      assertEquals(null, cacheFactory.getHotRodCache().put(key1, "v1", 5, TimeUnit.SECONDS));

      // 2. Put with Embedded
      assertEquals(null, cacheFactory.getEmbeddedCache().put(key2, "v2", 5, TimeUnit.SECONDS));

      // 3. HEAD with REST key1
      HttpMethod headKey1 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key1);
      cacheFactory.getRestClient().executeMethod(headKey1);
      assertEquals(HttpServletResponse.SC_OK, headKey1.getStatusCode());
      assertNotNull(headKey1.getResponseHeader("Expires"));

      // 4. HEAD with REST key2
      HttpMethod headKey2 = new HeadMethod(cacheFactory.getRestUrl() + "/" + key2);
      cacheFactory.getRestClient().executeMethod(headKey2);
      assertEquals(HttpServletResponse.SC_OK, headKey2.getStatusCode());
      assertNotNull(headKey2.getResponseHeader("Expires"));
   }

   /**
    * The class needs a getter for the attribute "name" so that it can be converted to JSON format
    * internally by the REST server.
    */
   static class Person implements Serializable {

      final String name;

      public Person(String name) {
         this.name = name;
      }

      public String getName() {
         return name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Person person = (Person) o;

         if (!name.equals(person.name)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }
   }

}
