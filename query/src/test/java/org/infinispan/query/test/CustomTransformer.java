/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.test;

import org.infinispan.query.Transformer;

import java.util.StringTokenizer;

public class CustomTransformer implements Transformer {
   @Override
   public Object fromString(String s) {
      StringTokenizer strtok = new StringTokenizer(s, ",");
      int[] ints = new int[3];
      int i = 0;
      while (strtok.hasMoreTokens()) {
         String token = strtok.nextToken();
         String[] contents = token.split("=");
         ints[i++] = Integer.parseInt(contents[1]);
      }
      return new CustomKey(ints[0], ints[1], ints[2]);
   }

   @Override
   public String toString(Object customType) {
      CustomKey ck = (CustomKey) customType;
      return "i=" + ck.i + ",j=" + ck.j + ",k=" + ck.k;
   }
}