package org.infinispan.it.osgi.persistence.file;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.Osgi;
import org.infinispan.test.TestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import java.io.File;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class SingleFileStoreFunctionalTest extends org.infinispan.persistence.file.SingleFileStoreFunctionalTest {

   private static String tmpDirectory;

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCorePlusTests(),
            junitBundles(),
            keepRuntimeFolder()
      );
   }

   @BeforeClass
   public static void setUpTmpDir() {
      tmpDirectory = TestingUtil.tmpDirectory(SingleFileStoreFunctionalTest.class);
   }

   @AfterClass
   public static void clearTmpDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Test
   public void testTwoCachesSameCacheStore() {
      super.testTwoCachesSameCacheStore();
   }

   @Test
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();
   }

   @Test
   public void testPreloadStoredAsBinary() {
      super.testPreloadStoredAsBinary();
   }

   @Test
   public void testRestoreAtomicMap() throws Exception {
      super.testRestoreAtomicMap(this.getClass().getMethod("testRestoreAtomicMap"));
   }

   @Test
   public void testRestoreTransactionalAtomicMap() throws Exception {
      super.testRestoreTransactionalAtomicMap(this.getClass().getMethod("testRestoreTransactionalAtomicMap"));
   }

   @Test
   public void testStoreByteArrays() throws Exception {
      super.testStoreByteArrays(this.getClass().getMethod("testStoreByteArrays"));
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence
            .addSingleFileStore()
            .location(tmpDirectory)
            .preload(preload);
      return persistence;
   }

   @Test
   public void testParsingEmptyElement() throws Exception {
      super.testParsingEmptyElement();
   }

   @Test
   public void testParsingElement() throws Exception {
      super.testParsingElement();
   }

}
