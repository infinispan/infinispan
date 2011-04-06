/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.distexec.mapreduce;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * 
 * 
 * @author Vladimir Blagojevic
 */
@Test
public abstract class BaseWordCountMapReduceTest extends MultipleCacheManagersTest {

   public BaseWordCountMapReduceTest() {
      cleanup = CleanupPhase.AFTER_TEST;
   }

   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }
   
   protected String cacheName(){
      return "mapreducecache";
   }
   
   private MapReduceTask<String, String, String, Integer> testinvokeMapReduce(String keys[]) throws Exception {
      Cache c1 = cache(0, cacheName());
      Cache c2 = cache(1, cacheName());

      c1.put("1", "Hello world here I am");
      c2.put("2", "Infinispan rules the world");
      c1.put("3", "JUDCon is in Boston");
      c2.put("4", "JBoss World is in Boston as well");
      c1.put("12","JBoss Application Server");
      c2.put("15", "Hello world");
      c1.put("14", "Infinispan community");
      c2.put("15", "Hello world");

      c1.put("111", "Infinispan open source");
      c2.put("112", "Boston is close to Toronto");
      c1.put("113", "Toronto is a capital of Ontario");
      c2.put("114", "JUDCon is cool");
      c1.put("211", "JBoss World is awesome");
      c2.put("212", "JBoss rules");
      c1.put("213", "JBoss division of RedHat ");
      c2.put("214", "RedHat community");

      MapReduceTask<String, String, String, Integer> task = new MapReduceTask<String, String, String, Integer>(c1);
      task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
      if(keys != null && keys.length>0){
         task.onKeys("1","2","3");
      }      
      return task; 
   }


   public void testinvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(null);
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 3;
      count = mapReduce.get("RedHat");
      assert count == 2;
   }
   
   public void testinvokeMapReduceOnAllKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(null);
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      Integer count = mapReduce.get("Infinispan");
      assert count == 3;
      count = mapReduce.get("RedHat");
      assert count == 2;
   }

   public void testinvokeMapReduceOnSubsetOfKeys() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(new String[] { "1", "2", "3" });
      Map<String, Integer> mapReduce = task.execute();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }
   
   public void testinvokeMapReduceOnSubsetOfKeysAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(new String[] { "1", "2", "3" });
      Future<Map<String, Integer>> future = task.executeAsynchronously();
      Map<String, Integer> mapReduce = future.get();
      Integer count = mapReduce.get("Infinispan");
      assert count == 1;
      count = mapReduce.get("Boston");
      assert count == 1;
   }
   
   public void testinvokeMapReduceOnAllKeysWithCollator() throws Exception {
       MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(null);
       Integer totalWords = task.execute(new Collator<String, Integer, Integer>() {
         
         @Override
         public Integer collate(Map<String, Integer> reducedResults) {
            int sum = 0;
            for (Entry<String, Integer> e : reducedResults.entrySet()) {
               sum += e.getValue();
            }
            return sum;
         }
      });
      assert totalWords == 56; 
   }

   public void testinvokeMapReduceOnSubsetOfKeysWithCollator() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(new String[] { "1", "2", "3" });
      Integer totalWords = task.execute(new Collator<String, Integer, Integer>() {
         
         @Override
         public Integer collate(Map<String, Integer> reducedResults) {
            int sum = 0;
            for (Entry<String, Integer> e : reducedResults.entrySet()) {
               sum += e.getValue();
            }
            return sum;
         }
      });     
      assert totalWords == 13;    
   }
   
   public void testinvokeMapReduceOnAllKeysWithCollatorAsync() throws Exception {
      MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(null);
      Future<Integer> future = task.executeAsynchronously(new Collator<String, Integer, Integer>() {
        
        @Override
        public Integer collate(Map<String, Integer> reducedResults) {
           int sum = 0;
           for (Entry<String, Integer> e : reducedResults.entrySet()) {
              sum += e.getValue();
           }
           return sum;
        }
     });
     Integer totalWords = future.get(); 
     assert totalWords == 56; 
  }

  public void testinvokeMapReduceOnSubsetOfKeysWithCollatorAsync() throws Exception {
     MapReduceTask<String,String,String,Integer> task = testinvokeMapReduce(new String[] { "1", "2", "3" });
     Future<Integer> future = task.executeAsynchronously(new Collator<String, Integer, Integer>() {
        
        @Override
        public Integer collate(Map<String, Integer> reducedResults) {
           int sum = 0;
           for (Entry<String, Integer> e : reducedResults.entrySet()) {
              sum += e.getValue();
           }
           return sum;
        }
     });
     Integer totalWords = future.get();
     assert totalWords == 13;    
  }

   private static class WordCountMapper implements Mapper<String, String, String,Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> collector) {
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

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         int sum = 0;
         while (iter.hasNext()) {
            Integer i = (Integer) iter.next();
            sum += i;
         }
         return sum;
      }
   }
}
