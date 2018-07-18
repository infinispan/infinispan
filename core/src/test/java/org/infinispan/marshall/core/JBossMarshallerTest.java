package org.infinispan.marshall.core;

import static org.infinispan.marshall.AdvancedExternalizerTest.IdViaAnnotationObj;
import static org.infinispan.marshall.AdvancedExternalizerTest.IdViaBothObj;
import static org.infinispan.marshall.AdvancedExternalizerTest.IdViaConfigObj;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the behaviour of JBoss Marshalling based {@link org.infinispan.commons.marshall.StreamingMarshaller} implementation
 * which is {@link JBossMarshaller}}. This class should contain methods that exercise
 * logic in this particular implementation.
 */
@Test(groups = "functional", testName = "marshall.jboss.JBossMarshallerTest")
public class JBossMarshallerTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(JBossMarshallerTest.class);

   private EmbeddedCacheManager cm;

   @BeforeClass
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager();
   }

   @AfterClass
   public void tearDown() {
      if (cm != null) cm.stop();
   }

   public void testInternalDuplicateExternalizerId() throws Exception {
      withExpectedInternalFailure(new DuplicateIdClass.Externalizer(), "Should have thrown a CacheException reporting the duplicate id");
   }

   public void testInternalExternalIdLimit() {
      withExpectedInternalFailure(new TooHighIdClass.Externalizer(), "Should have thrown a CacheException indicating that the Id is too high");
   }

   @Test(expectedExceptions=CacheException.class)
   public void testForeignExternalizerIdNegative() {
      GlobalConfigurationBuilder global = createForeignExternalizerGlobalConfig(-1);
      TestCacheManagerFactory.createCacheManager(global, new ConfigurationBuilder());
   }

   @Test(expectedExceptions=CacheConfigurationException.class)
   public void testForeignExternalizerIdClash() {
      createMultiForeignExternalizerGlobalConfig(3456, true);
   }

   @Test(expectedExceptions=CacheConfigurationException.class)
   public void testForeignExternalizerIdClash2() {
      createMultiForeignExternalizerGlobalConfig(5678, true);
   }

   @Test(expectedExceptions=CacheConfigurationException.class)
   public void testForeignExternalizerWithoutId() {
      createMultiForeignExternalizerGlobalConfig(9999, false);
   }

   public void testForeignExternalizerConfigIdWins() throws Exception {
      GlobalConfigurationBuilder globalCfg = createForeignExternalizerGlobalConfig(3456);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalCfg, new ConfigurationBuilder());
      try {
         cm.getCache();
         assertEquals(3456, findExternalizerId(new IdViaBothObj(), cm));
      } finally {
         cm.stop();
      }
   }

   private int findExternalizerId(Object obj, EmbeddedCacheManager cm) {
      GlobalMarshaller marshaller = TestingUtil.extractGlobalMarshaller(cm);
      return ((AdvancedExternalizer<?>) marshaller.findExternalizerFor(obj)).getId();
   }

   public void testForeignExternalizerMultiClassTypesViaSameExternalizer() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.serialization().addAdvancedExternalizer(new MultiIdViaClassExternalizer());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder, new ConfigurationBuilder());
      try {
         cm.getCache();
         assert 767 == findExternalizerId(new IdViaConfigObj(), cm);
         assert 767 == findExternalizerId(new IdViaAnnotationObj(), cm);
         assert 767 == findExternalizerId(new IdViaBothObj(), cm);
      } finally {
         cm.stop();
      }
   }

   public void testForeignExternalizerMultiClassNameTypesViaSameExternalizer() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.serialization().addAdvancedExternalizer(868, new MultiIdViaClassNameExternalizer());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder, new ConfigurationBuilder());
      try {
         cm.getCache();
         assert 868 == findExternalizerId(new IdViaConfigObj(), cm);
         assert 868 == findExternalizerId(new IdViaAnnotationObj(), cm);
         assert 868 == findExternalizerId(new IdViaBothObj(), cm);
      } finally {
         cm.stop();
      }
   }

   private GlobalConfigurationBuilder createForeignExternalizerGlobalConfig(int id) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.serialization().addAdvancedExternalizer(id, new IdViaBothObj.Externalizer());
      return builder;
   }

   private GlobalConfigurationBuilder createMultiForeignExternalizerGlobalConfig(int id, boolean doSetId) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();

      if (doSetId)
         builder.serialization().addAdvancedExternalizer(id, new IdViaConfigObj.Externalizer());
      else
         builder.serialization().addAdvancedExternalizer(new IdViaConfigObj.Externalizer());

      builder.serialization().addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer());
      builder.serialization().addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return builder;
   }

   private void withExpectedInternalFailure(final AdvancedExternalizer<?> ext, String message) {
      try {
         GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
         globalBuilder.serialization().addAdvancedExternalizer(ext).addAdvancedExternalizer(ext);
         assert false : message;
      } catch (CacheConfigurationException ce) {
         log.trace("Expected exception", ce);
      } finally {
         cm.stop();
      }
   }

   static class DuplicateIdClass {
      public static class Externalizer extends AbstractExternalizer<DuplicateIdClass> {
         @Override
         public DuplicateIdClass readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
            return null;
         }

         @Override
         public void writeObject(UserObjectOutput output, DuplicateIdClass object) throws IOException {
         }

         @Override
         public Integer getId() {
            return Ids.MAPS;
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
         public TooHighIdClass readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
            return null;
         }

         @Override
         public void writeObject(UserObjectOutput output, TooHighIdClass object) throws IOException {
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
      private final AdvancedExternalizer idViaConfigObjExt = new IdViaConfigObj.Externalizer();
      private final AdvancedExternalizer idViaAnnotationObjExt = new IdViaAnnotationObj.Externalizer();
      private final AdvancedExternalizer idViaBothObjExt = new IdViaBothObj.Externalizer();

      @Override
      public void writeObject(UserObjectOutput output, Object object) throws IOException {
         AdvancedExternalizer ext;
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
      public Object readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         int index = input.read();
         AdvancedExternalizer ext;
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
               Util.loadClass("org.infinispan.marshall.AdvancedExternalizerTest$IdViaConfigObj", Thread.currentThread().getContextClassLoader()),
               Util.loadClass("org.infinispan.marshall.AdvancedExternalizerTest$IdViaAnnotationObj", Thread.currentThread().getContextClassLoader()),
               Util.loadClass("org.infinispan.marshall.AdvancedExternalizerTest$IdViaBothObj", Thread.currentThread().getContextClassLoader()));
      }
   }
}
