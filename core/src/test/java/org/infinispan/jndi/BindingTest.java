/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.jndi;

import java.lang.reflect.Method;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.util.naming.NonSerializableFactory;
import org.jnp.server.Main;
import org.jnp.server.SingletonNamingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jndi.BindingTest")
public class BindingTest extends SingleCacheManagerTest {

   private Main namingMain;
   private SingletonNamingServer namingServer;
   private Properties props;

   @BeforeClass
   public void startJndiServer() throws Exception {
      // Create an in-memory jndi
      namingServer = new SingletonNamingServer();
      namingMain = new Main();
      namingMain.setInstallGlobalService(true);
      namingMain.setPort(-1);
      namingMain.start();
      props = new Properties();
      props.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      props.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
   }

   @AfterClass
   public void stopJndiServer() throws Exception {
      namingServer.destroy();
      namingMain.stop();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      Configuration configuration = new Configuration();
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);
      return cacheManager;
   }

   public void testBindCacheManagerToJndi() throws Exception {
      Context ctx = new InitialContext(props);
      String jndiName = "java:CacheManager";
      bind(jndiName, cacheManager, CacheContainer.class, ctx);
      try {
         Context ctx2 = new InitialContext(props);
         try {
            EmbeddedCacheManager cacheManager2 = (EmbeddedCacheManager) ctx2.lookup(jndiName);
            assert cacheManager.getStatus() == cacheManager2.getStatus();
         } finally {
            ctx2.close();
         }
      } finally {
         unbind(jndiName, ctx);
         ctx.close();
      }
   }

   public void testBindCacheToJndi(Method method) throws Exception {
      Context ctx = new InitialContext(props);
      String jndiName = "java:Cache";
      Cache cache = cacheManager.getCache(method.getName());
      bind(jndiName, cache, Cache.class, ctx);
      try {
         Context ctx2 = new InitialContext(props);
         try {
            Cache cache2 = (Cache) ctx2.lookup(jndiName);
            assert cache.getName() == cache2.getName();
         } finally {
            ctx2.close();
         }
      } finally {
         unbind(jndiName, ctx);
         ctx.close();
      }
   }

   /**
    * Helper method that binds the a non serializable object to the JNDI tree.
    * 
    * @param jndiName Name under which the object must be bound
    * @param who Object to bind in JNDI
    * @param classType Class type under which should appear the bound object
    * @param ctx Naming context under which we bind the object
    * @throws Exception Thrown if a naming exception occurs during binding
    */
   private void bind(String jndiName, Object who, Class<?> classType, Context ctx) throws Exception {
      // Ah ! This service isn't serializable, so we use a helper class
      NonSerializableFactory.bind(jndiName, who);
      Name n = ctx.getNameParser("").parse(jndiName);
      while (n.size() > 1) {
         String ctxName = n.get(0);
         try {
            ctx = (Context) ctx.lookup(ctxName);
         } catch (NameNotFoundException e) {
            log.debug("creating Subcontext " + ctxName);
            ctx = ctx.createSubcontext(ctxName);
         }
         n = n.getSuffix(1);
      }

      // The helper class NonSerializableFactory uses address type nns, we go on to
      // use the helper class to bind the service object in JNDI
      StringRefAddr addr = new StringRefAddr("nns", jndiName);
      Reference ref = new Reference(classType.getName(), addr, NonSerializableFactory.class.getName(), null);
      ctx.rebind(n.get(0), ref);
   }
   
   private void unbind(String jndiName, Context ctx) throws Exception {
      NonSerializableFactory.unbind(jndiName);
      ctx.unbind(jndiName);
   }
}
