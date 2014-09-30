package org.infinispan.cdi.test.distexec;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;

import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.Input;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.distexec.mapreduce.BaseWordCountMapReduceTest;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.SimpleTwoNodesMapReduceTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * BaseTest for MapReduceTask
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "cdi.test.distexec.WordCountMapReduceCDITest")
public class WordCountMapReduceCDITest extends MultipleCacheManagersArquillianTest {

   BaseWordCountMapReduceTest delegate;

   public WordCountMapReduceCDITest() {
      delegate = new SimpleTwoNodesMapReduceTest();
   }
   
   @Override
   MultipleCacheManagersTest getDelegate() {
      return delegate;
   }

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addClass(WordCountMapReduceCDITest.class)
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   public void testinvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String, String, String, Integer> task = delegate.invokeMapReduce(new String[] {
               "1", "2", "3" }, new WordCountMapper(), new WordCountReducer());
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }
   
   public void testinvokeMapReduceWithInputCacheOnSubsetOfKeys() throws Exception {
      MapReduceTask<String, String, String, Integer> task = delegate.invokeMapReduce(new String[] {
               "1", "2", "3" }, new WordCountImpliedInputCacheMapper(), new WordCountReducer());
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }

   private static class WordCountMapper implements Mapper<String, String, String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Inject
      private Cache<String, String> cache;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            collector.emit(s, 1);
         }
      }
   }
   
   private static class WordCountImpliedInputCacheMapper implements Mapper<String, String, String, Integer> {
    
      /** The serialVersionUID */
      private static final long serialVersionUID = 7525403183805551028L;
      
      @Inject
      @Input
      private Cache<String, String> cache;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         //the right cache injected         
         Assert.assertTrue(cache.getName().equals("mapreducecache")); 
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            collector.emit(s, 1);
         }
      }
   }

   private static class WordCountReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;

      @Inject
      private Cache<String, String> cache;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         Assert.assertNotNull(cache, "Cache not injected into " + this);
         int sum = 0;
         while (iter.hasNext()) {
            Integer i = iter.next();
            sum += i;
         }
         return sum;
      }
   }
}
