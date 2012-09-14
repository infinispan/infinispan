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
package org.infinispan.cdi.test.distexec;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;

import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.cdi.Input;
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
@Test(enabled = true, groups = "functional", testName = "distexec.WordCountMapReduceCDITest")
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
      return baseDeployment().addClass(WordCountMapReduceCDITest.class);
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
