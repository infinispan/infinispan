/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.rest;

import static org.testng.Assert.assertEquals;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.rest.ServerInstance.Client;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test with two REST Servers.
 * 
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@Test(groups = { "functional" }, testName = "rest.TwoServerTest")
public class TwoServerTest {

   private static final String PATH1 = "http://localhost:8888/rest/1-___defaultcache/";
   private static final String PATH2 = "http://localhost:8889/rest/2-___defaultcache/";

   @BeforeClass
   private void createServers() throws Exception {
      ServerInstance.addMultiServer("1", 8888, "infinispan1.xml");
      ServerInstance.addMultiServer("2", 8889, "infinispan2.xml");
      ServerInstance.start();
      Client.create();
   }

   @AfterClass
   private void destroyServers() throws Exception {
      ServerInstance.stop();
      ServerInstance.removeServers();
      Client.destroy();
   }

   public void testPutReplication() throws Exception {
      PutMethod put = new PutMethod(PATH1 + "a");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      Client.call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      GetMethod get = new GetMethod(PATH1 + "a");
      Client.call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      get.releaseConnection();
      get = new GetMethod(PATH2 + "a");
      Client.call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      assertEquals("data", get.getResponseBodyAsString());
      get.releaseConnection();
   }

   public void testReplace() throws Exception {
      PutMethod put = new PutMethod(PATH1 + "testReplace");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      Client.call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      put = new PutMethod(PATH1 + "testReplace");
      put.setRequestEntity(new StringRequestEntity("data2", "application/text", null));
      Client.call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      GetMethod get = new GetMethod(PATH2 + "testReplace");
      Client.call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      assertEquals("data2", get.getResponseBodyAsString());
      get.releaseConnection();
   }
}
