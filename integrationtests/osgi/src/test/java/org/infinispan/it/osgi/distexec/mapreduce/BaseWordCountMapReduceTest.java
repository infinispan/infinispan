package org.infinispan.it.osgi.distexec.mapreduce;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * @author mgencur
 */
public abstract class BaseWordCountMapReduceTest extends org.infinispan.distexec.mapreduce.BaseWordCountMapReduceTest {

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
   public static void setUpBeforeClass() throws Throwable {
      BaseWordCountMapReduceTest.specifyWordCounts();
   }

   @Test
   public void testInvokeMapReduceOnAllKeys() throws Exception {
      super.testInvokeMapReduceOnAllKeys();
   }

   @Test
   public void testInvokeMapReduceOnAllKeysWithResultCache() throws Exception {
      super.testInvokeMapReduceOnAllKeysWithResultCache();
   }

   @Test
   public void testInvokeMapReduceOnEmptyKeys() throws Exception {
      super.testInvokeMapReduceOnEmptyKeys();
   }

   @Test
   public void testInvokeMapReduceOnAllKeysWithCombiner() throws Exception {
      super.testInvokeMapReduceOnAllKeysWithCombiner();
   }

   @Test
   public void testCombinerDoesNotChangeResult() throws Exception {
      super.testCombinerDoesNotChangeResult();
   }

   @Test
   public void testMapperReducerIsolation() throws Exception{
      super.testMapperReducerIsolation();
   }

   @Test
   public void testInvokeMapReduceOnAllKeysAsync() throws Exception {
      super.testInvokeMapReduceOnAllKeysAsync();
   }

   @Test
   public void testInvokeMapReduceOnSubsetOfKeys() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeys();
   }

   @Test
   public void testInvokeMapReduceOnSubsetOfKeysWithResultCache() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeysWithResultCache();
   }

   @Test
   public void testInvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeysAsync();
   }

   @Test
   public void testInvokeMapReduceOnAllKeysWithCollator() throws Exception {
      super.testInvokeMapReduceOnAllKeysWithCollator();
   }

   @Test
   public void testInvokeMapReduceOnSubsetOfKeysWithCollator() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeysWithCollator();
   }

   @Test
   public void testInvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
      super.testInvokeMapReduceOnAllKeysWithCollatorAsync();
   }

   @Test
   public void testInvokeMapReduceOnSubsetOfKeysWithCollatorAsync() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeysWithCollatorAsync();
   }

   @Test(expected = CacheException.class)
   public void testCombinerForDistributedReductionWithException() throws Exception {
      super.testCombinerForDistributedReductionWithException();
   }
}