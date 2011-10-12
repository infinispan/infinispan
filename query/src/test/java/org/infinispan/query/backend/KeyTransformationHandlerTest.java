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
package org.infinispan.query.backend;

import org.infinispan.query.test.*;
import org.testng.annotations.Test;

/**
 * This is the test class for {@link org.infinispan.query.backend.KeyTransformationHandler}
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */

@Test(groups = "functional")
public class KeyTransformationHandlerTest {

   String s = null;
   Object key = null;

   public void testKeyToStringWithStringAndPrimitives() {
      s = KeyTransformationHandler.keyToString("key");
      assert s.equals("S:key");

      s = KeyTransformationHandler.keyToString(1);
      assert s.equals("I:1");

      s = KeyTransformationHandler.keyToString(true);
      assert s.equals("B:true");

      s = KeyTransformationHandler.keyToString((short) 1);
      assert s.equals("X:1");

      s = KeyTransformationHandler.keyToString((long) 1);
      assert s.equals("L:1");

      s = KeyTransformationHandler.keyToString((byte) 1);
      assert s.equals("Y:1");

      s = KeyTransformationHandler.keyToString((float) 1);
      assert s.equals("F:1.0");

      s = KeyTransformationHandler.keyToString('A');
      assert s.equals("C:A");

      s = KeyTransformationHandler.keyToString(1.0);
      assert s.equals("D:1.0");
   }

   public void testStringToKeyWithStringAndPrimitives() {
      key = KeyTransformationHandler.stringToKey("S:key1", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(String.class);
      assert key.equals("key1");

      key = KeyTransformationHandler.stringToKey("I:2", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Integer.class);
      assert key.equals(2);

      key = KeyTransformationHandler.stringToKey("Y:3", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Byte.class);
      assert key.equals((byte) 3);

      key = KeyTransformationHandler.stringToKey("F:4.0", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Float.class);
      assert key.equals((float) 4.0);

      key = KeyTransformationHandler.stringToKey("L:5", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Long.class);
      assert key.equals((long) 5);

      key = KeyTransformationHandler.stringToKey("X:6", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Short.class);
      assert key.equals((short) 6);

      key = KeyTransformationHandler.stringToKey("B:true", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Boolean.class);
      assert key.equals(true);

      key = KeyTransformationHandler.stringToKey("D:8.0", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Double.class);
      assert key.equals(8.0);

      key = KeyTransformationHandler.stringToKey("C:9", Thread.currentThread().getContextClassLoader());
      assert key.getClass().equals(Character.class);
      assert key.equals('9');

   }

   public void testStringToKeyWithCustomTransformable() {
      CustomKey customKey = new CustomKey(88, 8800, 12889976);
      String strRep = KeyTransformationHandler.keyToString(customKey);
      assert customKey.equals(KeyTransformationHandler.stringToKey(strRep, Thread.currentThread().getContextClassLoader()));
   }

   public void testStringToKeyWithDefaultTransformer() {
      CustomKey2 ck2 = new CustomKey2(Integer.MAX_VALUE, Integer.MIN_VALUE, 0);
      String strRep = KeyTransformationHandler.keyToString(ck2);
      assert ck2.equals(KeyTransformationHandler.stringToKey(strRep, Thread.currentThread().getContextClassLoader()));
   }

   public void testStringToKeyWithRegisteredTransformer() {
      KeyTransformationHandler.registerTransformer(CustomKey3.class, CustomKey3Transformer.class);

      CustomKey3 key = new CustomKey3("str");
      String string = KeyTransformationHandler.keyToString(key);
      assert key.equals(KeyTransformationHandler.stringToKey(string, Thread.currentThread().getContextClassLoader()));
   }

}
