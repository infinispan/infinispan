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

import org.infinispan.distexec.mapreduce.Reducer;

import java.util.Iterator;

public class WordCountReducer implements Reducer<String, Integer> {

   private static final long serialVersionUID = 1901016598354633256L;

   @Override
   public Integer reduce(String key, Iterator<Integer> iter) {
      int sum = 0;
      while (iter.hasNext()) {
         Integer i = iter.next();
         sum += i;
      }
      return sum;
   }
}
