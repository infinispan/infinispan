/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.distexec.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;

public class WordCountExample {
   public static void main(String arg[]) throws Exception {

      Cache <String, String> cache = null;
      MapReduceTask<String, String, Map<String, Integer>, Map<String, Integer>> task = new MapReduceTask<String, String, Map<String, Integer>, Map<String, Integer>>(cache);

      Map<String, Integer> result = task.onKeys("eviction", "hashing", "L1").mappedWith(new Mapper<String, String, Map<String, Integer>>() {

                  @Override
                  public Map<String, Integer> map(String key, String value) {
                     Map<String, Integer> counts = new HashMap<String, Integer>();
                     StringTokenizer tz = new StringTokenizer(value);
                     while (tz.hasMoreTokens()) {
                        String word = tz.nextToken();
                        Integer count = counts.get(word);
                        if (count == null) {
                           count = counts.put(word, 1);
                        } else {
                           counts.put(word, count++);
                        }
                     }
                     return counts;
                  }
               }).reducedWith(new Reducer<Map<String, Integer>, Map<String, Integer>>() {
                
                  @Override
                  public Map<String, Integer> reduce(Map<String, Integer> mapResult, Map<String, Integer> previousReduced) {
                     if(previousReduced != null && mapResult != null) {
                        for (Entry<String, Integer> e : mapResult.entrySet()) {
                           if(previousReduced.containsKey(e.getKey())){
                              previousReduced.put(e.getKey(), e.getValue() + previousReduced.get(e.getKey()));
                           } else {
                              previousReduced.put(e.getKey(), e.getValue());
                           }                           
                        }
                     }
                     return previousReduced;
                  }
               }).collate(new WordCountCollator());

      for (Entry<String, Integer> e : result.entrySet()) {
         System.out.println("For word " + e.getKey() + " count in all traversed documents is "
                  + e.getValue());
      }
   }

   static class WordCountCollator implements Collator<Map<String, Integer>> {

      Map<String, Integer> collatedResult = new HashMap<String, Integer>();

      @Override
      public Map<String, Integer> collate() {
         return collatedResult;
      }

      @Override
      public void reducedResultReceived(Address remoteNode, Map<String, Integer> remoteResult) {
         if(remoteResult != null) {
            for (Entry<String, Integer> e : remoteResult.entrySet()) {
               if(collatedResult.containsKey(e.getKey())){
                  collatedResult.put(e.getKey(), e.getValue() + collatedResult.get(e.getKey()));
               } else {
                  collatedResult.put(e.getKey(), e.getValue());
               }                           
            }
         }
      }
   }
}
