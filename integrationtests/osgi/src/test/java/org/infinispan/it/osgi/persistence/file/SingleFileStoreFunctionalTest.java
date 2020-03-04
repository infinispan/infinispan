package org.infinispan.it.osgi.persistence.file;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.util.CustomPaxExamRunner;
import org.infinispan.commons.test.TestResourceTracker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(CustomPaxExamRunner.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class SingleFileStoreFunctionalTest extends org.infinispan.persistence.file.SingleFileStoreFunctionalTest {
   private static String tmpDirectory;

   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @BeforeClass
   public static void setUpTmpDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(SingleFileStoreFunctionalTest.class);
   }

   @AfterClass
   public static void clearTmpDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Before
   @Override
   public void setup() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      super.setup();
   }

   @After
   @Override
   public void teardown() {
      super.teardown();
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
