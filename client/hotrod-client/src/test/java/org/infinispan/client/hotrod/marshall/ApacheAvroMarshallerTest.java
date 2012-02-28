/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod.marshall;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Test(groups = "functional", testName = "client.hotrod.ApacheAvroMarshallerTest")
public class ApacheAvroMarshallerTest {

   private final ApacheAvroMarshaller marshaller = new ApacheAvroMarshaller();

   public void testStringMarshalling() {
      String x = "Galder";
      String y = (String) marshallUnmarshall(x);
      assert y.equals(x);
   }

   public void testBooleanMarshalling() {
      boolean x = true;
      boolean y = ((Boolean) marshallUnmarshall(x)).booleanValue();
      assert x == y;
      assertEquality(new Boolean(false));
   }

   public void testIntegerMarshalling() {
      int x = 99;
      int y = ((Integer) marshallUnmarshall(x)).intValue();
      assert x == y;
      assertEquality(new Integer(12345));
   }

   public void testLongMarshalling() {
      long x = 9223372036854775807L;
      long y = ((Long) marshallUnmarshall(x)).longValue();
      assert x == y;
      assertEquality(new Long(72057594037927936L));
   }

   public void testFloatMarshalling() {
      float x = 123.4f;
      float y = ((Float) marshallUnmarshall(x)).floatValue();
      assert x == y;
      assertEquality(new Float(56789.9));
   }

   public void testDoubleMarshalling() {
      double x = 1.234e2;
      double y = ((Double) marshallUnmarshall(x)).doubleValue();
      assert x == y;
      assertEquality(new Double(5.678e9));
   }

   public void testNullMarshalling() {
      assert null == marshallUnmarshall(null);
   }

   public void testBytesMarshalling() {
      byte[] x = new byte[]{1, 2, 3, 4};
      byte[] y = (byte[]) marshallUnmarshall(x);
      assert Arrays.equals(x, y);
   }

   public void testStringArrayMarshalling() {
      assertArrayEquality(new String[]{"Basque Country", "Spain", "UK", "Switzerland"});
   }

   public void testIntArrayMarshalling() {
      assertArrayEquality(new Integer[]{1234, 5678, 9101112});
   }

   public void testLongArrayMarshalling() {
      assertArrayEquality(new Long[]{9223372036854775807L, 72057594037927936L});
   }

   public void testBooleanArrayMarshalling() {
      assertArrayEquality(new Boolean[] {true, false, true, true});
   }

   public void testFloatArrayMarshalling() {
      assertArrayEquality(new Float[] {56789.9f, 1234.6f, 85894.303f, 67484.32f, 4732.4f});
   }

   public void testDoubleArrayMarshalling() {
      assertArrayEquality(new Double[] {5.678e9, 1.623435e9, 5.654545e5, 9.6232323e1});
   }

   public void testListMarshalling() {
      List<String> cities = new ArrayList<String>();
      cities.add("algorta");
      cities.add("neuchatel");
      cities.add("ibiza");
      assertEquality(cities);

      List<Integer> numbers = new ArrayList<Integer>();
      numbers.add(12);
      numbers.add(3232412);
      numbers.add(4345132);
      numbers.add(898979);
      assertEquality(numbers);

      List<Boolean> testimony = new LinkedList<Boolean>();
      testimony.add(false);
      testimony.add(true);
      testimony.add(true);
      testimony.add(true);
      assertEquality(testimony);
   }

   public void testMapMarshalling() {
      Map<Long, Float> numbers = new HashMap<Long, Float>();
      numbers.put(9223372036854775807L, 4732.4f);
      numbers.put(72057594037927936L, 67484.32f);
      numbers.put(7205759412424936L, 132367484.32f);
      assertEquality(numbers);
   }

   public void testSetMarshalling() {
      Set words = new HashSet();
      words.add("cat");
      words.add("dog");
      words.add("perro");
      words.add("txakur");
      assertEquality(words);
   }

   private Object marshallUnmarshall(Object o) {
      try {
         byte[] buffer = marshaller.objectToByteBuffer(o);
         return marshaller.objectFromByteBuffer(buffer);
      } catch(Exception e) {
         throw new RuntimeException("Error marshalling or unmarshalling", e);
      }
   }

   private <T> void assertArrayEquality(T[] x) {
      T[] y = (T[]) ApacheAvroMarshallerTest.this.marshallUnmarshall(x);
      assert Arrays.equals(x, y);
   }

   private <T> void assertEquality(T x) {
      T y = (T) ApacheAvroMarshallerTest.this.marshallUnmarshall(x);
      assert x.equals(y);
   }

}
