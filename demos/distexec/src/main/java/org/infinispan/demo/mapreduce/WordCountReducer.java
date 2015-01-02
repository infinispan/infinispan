package org.infinispan.demo.mapreduce;

import org.infinispan.distexec.mapreduce.Reducer;

import java.util.Iterator;

public class WordCountReducer implements Reducer<String, Integer> {

   private static final long serialVersionUID = 1901016598354633256L;

   @Override
   public Integer reduce(String key, Iterator<Integer> iter) {
      // Set Thread Name
      Thread.currentThread().setName(String.format("ReducerThread-%d", Thread.currentThread().getId()));
      
      int sum = 0;
      while (iter.hasNext()) {
         Integer i = iter.next();
         sum += i;
      }
      return sum;
   }
}
