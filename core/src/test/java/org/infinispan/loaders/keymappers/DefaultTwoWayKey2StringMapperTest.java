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
package org.infinispan.loaders.keymappers;

import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.keymappers.DefaultTwoWayKey2StringMapperTest")
public class DefaultTwoWayKey2StringMapperTest {

   DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();

   public void testKeyMapper() {
      String skey = mapper.getStringMapping("k1");
      assert skey.equals("k1");

      skey = mapper.getStringMapping(Integer.valueOf(100));

      assert !skey.equals("100");

      Integer i = (Integer) mapper.getKeyMapping(skey);
      assert i == 100;

      skey = mapper.getStringMapping(Boolean.TRUE);

      assert !skey.equalsIgnoreCase("true");

      Boolean b = (Boolean) mapper.getKeyMapping(skey);

      assert b;

      skey = mapper.getStringMapping(Double.valueOf(3.141592d));

      assert !skey.equals("3.141592");

      Double d = (Double) mapper.getKeyMapping(skey);

      assert d == 3.141592d;

      byte[] bytes = new byte[] { 0, 1, 2, 40, -128, -127, 127, 126, 0 };

      skey = mapper.getStringMapping(new ByteArrayKey(bytes));

      assert !skey.equals("\000\001\002\050\0377\0376\0177\0176\000");
   }

   public void testPrimitivesAreSupported() {
      assert mapper.isSupportedType(Integer.class);
      assert mapper.isSupportedType(Byte.class);
      assert mapper.isSupportedType(Short.class);
      assert mapper.isSupportedType(Long.class);
      assert mapper.isSupportedType(Double.class);
      assert mapper.isSupportedType(Float.class);
      assert mapper.isSupportedType(Boolean.class);
      assert mapper.isSupportedType(String.class);
      assert mapper.isSupportedType(ByteArrayKey.class);
   }

   public void testTwoWayContract() {
      Object[] toTest = { 0, new Byte("1"), new Short("2"), (long) 3, new Double("3.4"), new Float("3.5"), Boolean.FALSE, "some string", new ByteArrayKey("\000\001\002\050\0377\0376\0177\0176\000".getBytes()) };
      for (Object o : toTest) {
         Class<?> type = o.getClass();
         String rep = mapper.getStringMapping(o);
         assert o.equals(mapper.getKeyMapping(rep)) : String.format("Failed on type %s and value %s", type, rep);
      }
   }

   public void testAssumption() {
      // even if they have the same value, they have a different type
      assert !new Float(3.0f).equals(new Integer(3));
   }

   public void testString() {
      assert mapper.isSupportedType(String.class);
      assert assertWorks("") : "Expected empty string, was " + mapper.getStringMapping("");
      assert assertWorks("mircea") : "Expected 'mircea', was " + mapper.getStringMapping("mircea");
   }

   public void testShort() {
      assert mapper.isSupportedType(Short.class);
      assert assertWorks((short) 2);
   }

   public void testByte() {
      assert mapper.isSupportedType(Byte.class);
      assert assertWorks((byte) 2);
   }

   public void testLong() {
      assert mapper.isSupportedType(Long.class);
      assert assertWorks(new Long(2));
   }

   public void testInteger() {
      assert mapper.isSupportedType(Integer.class);
      assert assertWorks(2);
   }

   public void testDouble() {
      assert mapper.isSupportedType(Double.class);
      assert assertWorks(2.4d);

   }

   public void testFloat() {
      assert mapper.isSupportedType(Float.class);
      assert assertWorks(2.1f);

   }

   public void testBoolean() {
      assert mapper.isSupportedType(Boolean.class);
      assert assertWorks(true);
      assert assertWorks(false);
   }

   public void testByteArrayKey() {
      assert mapper.isSupportedType(ByteArrayKey.class);
      assert assertWorks(new ByteArrayKey("\000\001\002\050\0377\0376\0177\0176\000".getBytes()));
   }

   private boolean assertWorks(Object key) {
      return mapper.getKeyMapping(mapper.getStringMapping(key)).equals(key);
   }
}
