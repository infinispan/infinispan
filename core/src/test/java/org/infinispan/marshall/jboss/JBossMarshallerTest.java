/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.ExternalizerConfig;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.infinispan.marshall.ForeignExternalizerTest.*;

/**
 * Test the behaviour of JBoss Marshalling based {@link org.infinispan.marshall.StreamingMarshaller} implementation
 * which is {@link JBossMarshaller}}. This class should contain methods that exercise
 * logic in this particular implementation.
 */
@Test(groups = "functional", testName = "marshall.jboss.JBossMarshallerTest")
public class JBossMarshallerTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(JBossMarshallerTest.class);

   private final VersionAwareMarshaller marshaller = new VersionAwareMarshaller();

   @BeforeTest
   public void setUp() {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      marshaller.inject(cl, new RemoteCommandsFactory(), new GlobalConfiguration());
      marshaller.start();
   }

   @AfterTest
   public void tearDown() {
      marshaller.stop();
   }
   
   public void testInternalDuplicateExternalizerId() throws Exception {
      withExpectedInternalFailure(new DuplicateIdClass.Externalizer(), "Should have thrown a CacheException reporting the duplicate id");
   }

   public void testInternalExternalIdLimit() {
      withExpectedInternalFailure(new TooHighIdClass.Externalizer(), "Should have thrown a CacheException indicating that the Id is too high");
   }

   public void testForeignExternalizerIdNegative() {
      withExpectedFailure(createForeignExternalizerGlobalConfig(-1),
                          "Should have thrown a CacheException reporting that negative ids are not allowed");
   }

   public void testForeignExternalizerIdClash() {
      withExpectedFailure(createMultiForeignExternalizerGlobalConfig(3456, true),
                          "Should have thrown a CacheException reporting duplicate id");
      withExpectedFailure(createMultiForeignExternalizerGlobalConfig(5678, true),
                          "Should have thrown a CacheException reporting duplicate id");
   }

   public void testForeignExternalizerWithoutId() {
      withExpectedFailure(createMultiForeignExternalizerGlobalConfig(9999, false),
                          "Should have thrown a CacheException reporting that no id has been set");
   }

   public void testForeignExternalizerConfigIdWins() throws Exception {
      GlobalConfiguration globalCfg = createForeignExternalizerGlobalConfig(3456);
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
         jbmarshaller.start(cl, new RemoteCommandsFactory(), null, globalCfg);
         assert 3456 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaBothObj());
      } finally {
         jbmarshaller.stop();
      }
   }

   public void testForeignExternalizerMultiClassTypesViaSameExternalizer() {
      GlobalConfiguration globalCfg = new GlobalConfiguration();
      globalCfg.addExternalizer(new MultiIdViaClassExternalizer());
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
         jbmarshaller.start(cl, new RemoteCommandsFactory(), null, globalCfg);
         assert 767 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaConfigObj());
         assert 767 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaAnnotationObj());
         assert 767 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaBothObj());
      } finally {
         jbmarshaller.stop();
      }
   }

   public void testForeignExternalizerMultiClassNameTypesViaSameExternalizer() {
      GlobalConfiguration globalCfg = new GlobalConfiguration();
      globalCfg.addExternalizer(868, new MultiIdViaClassNameExternalizer());
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
         jbmarshaller.start(cl, new RemoteCommandsFactory(), null, globalCfg);
         assert 868 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaConfigObj());
         assert 868 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaAnnotationObj());
         assert 868 == jbmarshaller.externalizerTable.getExternalizerId(new IdViaBothObj());
      } finally {
         jbmarshaller.stop();
      }
   }

   private GlobalConfiguration createForeignExternalizerGlobalConfig(int id) {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      List<ExternalizerConfig> list = new ArrayList<ExternalizerConfig>();
      GlobalConfiguration.ExternalizersType type = new GlobalConfiguration.ExternalizersType();
      ExternalizerConfig externalizer = new ExternalizerConfig();
      externalizer.setId(id);
      externalizer.setExternalizerClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaBothObj$Externalizer");
      list.add(externalizer);
      type.setExternalizerConfigs(list);
      globalCfg.setExternalizersType(type);
      return globalCfg;
   }

   private GlobalConfiguration createMultiForeignExternalizerGlobalConfig(int id, boolean doSetId) {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      if (doSetId)
         globalCfg.addExternalizer(id, new IdViaConfigObj.Externalizer());
      else
         globalCfg.addExternalizer(new IdViaConfigObj.Externalizer());

      globalCfg.addExternalizer(new IdViaAnnotationObj.Externalizer());
      globalCfg.addExternalizer(3456, new IdViaBothObj.Externalizer());
      return globalCfg;
   }

   private void withExpectedInternalFailure(final Externalizer ext, String message) {
      JBossMarshaller jbmarshaller = new JBossMarshaller() {
         @Override
         protected ExternalizerTable createExternalizerTable(RemoteCommandsFactory f, StreamingMarshaller m, GlobalConfiguration g) {
            ExternalizerTable objectTable = new ExternalizerTable();
            objectTable.addInternalExternalizer(ext);
            objectTable.start(f, m, g);
            return objectTable;
         }
      };
      try {
         ClassLoader cl = Thread.currentThread().getContextClassLoader();
         jbmarshaller.start(cl, new RemoteCommandsFactory(), marshaller, new GlobalConfiguration());
         assert false : message;
      } catch (ConfigurationException ce) {
         log.trace("Expected exception", ce);
      } finally {
         jbmarshaller.stop();
      }
   }

   private void withExpectedFailure(GlobalConfiguration globalCfg, String message) {
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
         jbmarshaller.start(cl, new RemoteCommandsFactory(), null, globalCfg);
         assert false : message;
      } catch (ConfigurationException ce) {
         log.trace("Expected exception", ce);
      } finally {
         jbmarshaller.stop();
      }
   }

   static class DuplicateIdClass {
      public static class Externalizer extends AbstractExternalizer<DuplicateIdClass> {
         @Override
         public DuplicateIdClass readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return null;
         }

         @Override
         public void writeObject(ObjectOutput output, DuplicateIdClass object) throws IOException {
         }

         @Override
         public Integer getId() {
            return Ids.ARRAY_LIST;
         }

         @Override
         public Set<Class<? extends DuplicateIdClass>> getTypeClasses() {
            return Util.<Class<? extends DuplicateIdClass>>asSet(DuplicateIdClass.class);
         }
      }
   }

   static class TooHighIdClass {
      public static class Externalizer extends AbstractExternalizer<TooHighIdClass> {
         @Override
         public TooHighIdClass readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return null;
         }

         @Override
         public void writeObject(ObjectOutput output, TooHighIdClass object) throws IOException {
         }

         @Override
         public Integer getId() {
            return 255;
         }

         @Override
         public Set<Class<? extends TooHighIdClass>> getTypeClasses() {
            return Util.<Class<? extends TooHighIdClass>>asSet(TooHighIdClass.class);
         }
      }
   }

   static class MultiIdViaClassExternalizer extends AbstractExternalizer<Object> {
      private final Externalizer idViaConfigObjExt = new IdViaConfigObj.Externalizer();
      private final Externalizer idViaAnnotationObjExt = new IdViaAnnotationObj.Externalizer();
      private final Externalizer idViaBothObjExt = new IdViaBothObj.Externalizer();

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         Externalizer ext;
         if (object instanceof IdViaConfigObj) {
            output.write(0);
            ext = idViaConfigObjExt;
         } else if (object instanceof IdViaAnnotationObj) {
            output.write(1);
            ext = idViaAnnotationObjExt;
         } else if (object instanceof IdViaBothObj){
            output.write(2);
            ext = idViaBothObjExt;
         } else {
            throw new CacheException(String.format(
                  "Object of type %s is not supported by externalizer %s",
                  object.getClass().getName(), this.getClass().getName()));
         }
         ext.writeObject(output, object);
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int index = input.read();
         Externalizer ext;
         switch (index) {
            case 0:
               ext = idViaConfigObjExt;
               break;
            case 1:
               ext = idViaAnnotationObjExt;
               break;
            case 2:
               ext = idViaBothObjExt;
               break;
            default:
               throw new CacheException(String.format(
                     "Unknown index (%d) for externalizer %s",
                     index, this.getClass().getName()));
         }
         return ext.readObject(input);
      }

      @Override
      public Integer getId() {
         return 767;
      }

      @Override
      public Set<Class<? extends Object>> getTypeClasses() {
         return Util.asSet(IdViaConfigObj.class, IdViaAnnotationObj.class, IdViaBothObj.class);
      }
   }

   static class MultiIdViaClassNameExternalizer extends MultiIdViaClassExternalizer {
      @Override
      public Integer getId() {
         // Revert to default so that it can be retrieved from config
         return null;
      }

      @Override
      public Set<Class<? extends Object>> getTypeClasses() {
         return Util.<Class<? extends Object>>asSet(
               Util.loadClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaConfigObj"),
               Util.loadClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaAnnotationObj"),
               Util.loadClass("org.infinispan.marshall.ForeignExternalizerTest$IdViaBothObj"));
      }
   }
}
