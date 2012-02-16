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

package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Set;

/**
 * Tests configuration of user defined {@link AdvancedExternalizer} implementations
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdvancedExternalizerTest")
public class AdvancedExternalizerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalCfg1 = createForeignExternalizerGlobalConfig();
      GlobalConfigurationBuilder globalCfg2 = createForeignExternalizerGlobalConfig();
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      CacheContainer cm1 = TestCacheManagerFactory.createCacheManager(globalCfg1, cfg);
      CacheContainer cm2 = TestCacheManagerFactory.createCacheManager(globalCfg2, cfg);
      registerCacheManager(cm1, cm2);
      defineConfigurationOnAllManagers(getCacheName(), cfg);
      waitForClusterToForm(getCacheName());
   }

   protected String getCacheName() {
      return "ForeignExternalizers";
   }

   protected GlobalConfigurationBuilder createForeignExternalizerGlobalConfig() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.serialization()
         .addAdvancedExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer())
         .addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return builder;
   }

   public void testReplicatePojosWithUserDefinedExternalizers(Method m) {
      Cache cache1 = manager(0).getCache(getCacheName());
      Cache cache2 = manager(1).getCache(getCacheName());
      IdViaConfigObj configObj = new IdViaConfigObj().setName("Galder");
      String key = "k-" + m.getName() + "-viaConfig";
      cache1.put(key, configObj);
      assert configObj.name.equals(((IdViaConfigObj)cache2.get(key)).name);
      IdViaAnnotationObj annotationObj = new IdViaAnnotationObj().setDate(new Date(System.currentTimeMillis()));
      key = "k-" + m.getName() + "-viaAnnotation";
      cache1.put(key, annotationObj);
      assert annotationObj.date.equals(((IdViaAnnotationObj)cache2.get(key)).date);
      IdViaBothObj bothObj = new IdViaBothObj().setAge(30);
      key = "k-" + m.getName() + "-viaBoth";
      cache1.put(key, bothObj);
      assert bothObj.age == ((IdViaBothObj)cache2.get(key)).age;
   }

   public static class IdViaConfigObj {
      String name;

      public IdViaConfigObj setName(String name) {
         this.name = name;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaConfigObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaConfigObj object) throws IOException {
            output.writeUTF(object.name);
         }

         @Override
         public IdViaConfigObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaConfigObj().setName(input.readUTF());
         }

         @Override
         public Set<Class<? extends IdViaConfigObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaConfigObj>>asSet(IdViaConfigObj.class);
         }
      }
   }

   public static class IdViaAnnotationObj {
      Date date;

      public IdViaAnnotationObj setDate(Date date) {
         this.date = date;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaAnnotationObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaAnnotationObj object) throws IOException {
            output.writeObject(object.date);
         }

         @Override
         public IdViaAnnotationObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaAnnotationObj().setDate((Date) input.readObject());
         }

         @Override
         public Integer getId() {
            return 5678;
         }

         @Override
         public Set<Class<? extends IdViaAnnotationObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaAnnotationObj>>asSet(IdViaAnnotationObj.class);
         }
      }
   }

   public static class IdViaBothObj {
      int age;

      public IdViaBothObj setAge(int age) {
         this.age = age;
         return this;
      }

      public static class Externalizer extends AbstractExternalizer<IdViaBothObj> {
         @Override
         public void writeObject(ObjectOutput output, IdViaBothObj object) throws IOException {
            output.writeInt(object.age);
         }

         @Override
         public IdViaBothObj readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdViaBothObj().setAge(input.readInt());
         }

         @Override
         public Integer getId() {
            return 9012;
         }

         @Override
         public Set<Class<? extends IdViaBothObj>> getTypeClasses() {
            return Util.<Class<? extends IdViaBothObj>>asSet(IdViaBothObj.class);
         }
      }
   }

}
