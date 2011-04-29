/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.demo.mapreduce;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

public class WordCountMapper implements Mapper<String, String, String, Integer> {
   private static final long serialVersionUID = -5943370243108735560L;
   private static int chunks = 0, words = 0;

   @Override
   public void map(String key, String value, Collector<String, Integer> c) {
      chunks++;
      for (String word : value.split(" ")) {
         if (word != null) {
            String w = word.trim();
            if (w.length() > 0) {
               c.emit(word.toLowerCase(), 1);
               words++;
            }
         }
      }

      if (chunks % 1000 == 0) System.out.printf("Analyzed %s words in %s lines%n", words, chunks);
   }
}