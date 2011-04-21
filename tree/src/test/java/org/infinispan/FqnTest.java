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

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.infinispan;


import org.infinispan.tree.Fqn;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;

@Test(groups = "unit", testName = "FqnTest")
public class FqnTest {
   public void testNull() {
      Fqn fqn = Fqn.ROOT;
      assert 0 == fqn.size();
      int hcode = fqn.hashCode();
      assert hcode != -1;
   }

   public void testOne() {
      Fqn fqn = Fqn.fromElements(22);
      assert 1 == fqn.size();
      int hcode = fqn.hashCode();
      assert hcode != -1;
   }

   public void testEmptyFqn() {
      Fqn f1 = Fqn.ROOT;
      Fqn f2 = Fqn.ROOT;
      assert f1.equals(f2);
   }

   public void testFqn() {
      Fqn fqn = Fqn.fromString("/a/b/c");
      assert 3 == fqn.size();

      Fqn fqn2 = Fqn.fromElements("a", "b", "c");
      assert 3 == fqn.size();
      assert fqn.equals(fqn2);
      assert fqn.hashCode() == fqn2.hashCode();
   }

   public void testHereogeneousNames() {
      Fqn fqn = Fqn.fromElements("string", 38, true);
      assert 3 == fqn.size();

      Fqn fqn2 = Fqn.fromElements("string", 38, true);
      assert fqn.equals(fqn2);
      assert fqn.hashCode() == fqn2.hashCode();
   }

   public void testHashcode() {
      Fqn fqn1, fqn2;
      fqn1 = Fqn.fromElements("a", "b", "c");
      fqn2 = Fqn.fromString("/a/b/c");
      assert fqn1.equals(fqn2);

      HashMap<Fqn, Integer> map = new HashMap<Fqn, Integer>();
      map.put(fqn1, 33);
      map.put(fqn2, 34);
      assert map.size() == 1;
      assert map.get(fqn1).equals(34);
   }

   public void testEquals() {
      Fqn fqn1 = Fqn.fromElements("person/test");

      Fqn f1, f2, f3;

      f1 = Fqn.fromRelativeElements(fqn1, "0");
      f2 = Fqn.fromRelativeElements(fqn1, "1");
      f3 = Fqn.fromRelativeElements(fqn1, "2");

      HashMap<Fqn, String> map = new HashMap<Fqn, String>();
      map.put(f1, "0");
      map.put(f2, "1");
      map.put(f3, "2");

      assert map.get(Fqn.fromRelativeElements(fqn1, "0")) != null;
      assert map.get(Fqn.fromRelativeElements(fqn1, "1")) != null;
      assert map.get(Fqn.fromRelativeElements(fqn1, "2")) != null;

   }

   public void testEquals2() {
      Fqn f1;
      Fqn f2;
      f1 = Fqn.fromString("/a/b/c");
      f2 = Fqn.fromString("/a/b/c");
      assert f1.equals(f2);

      f2 = Fqn.fromString("/a/b");
      assert !f1.equals(f2);

      f2 = Fqn.fromString("/a/b/c/d");
      assert !f1.equals(f2);
   }

   public void testEquals3() {
      Fqn f1;
      Fqn f2;
      f1 = Fqn.fromElements("a", 322649, Boolean.TRUE);
      f2 = Fqn.ROOT;
      assert !f1.equals(f2);
      assert !f2.equals(f1);

      f2 = Fqn.fromString("a/322649/TRUE");
      assert !f1.equals(f2);

      f2 = Fqn.fromElements("a", 322649, Boolean.FALSE);
      assert !f1.equals(f2);

      f2 = Fqn.fromElements("a", 322649, Boolean.TRUE);
      assert f1.equals(f2);
   }

   public void testEquals4() {
      Fqn fqn = Fqn.fromString("X");
      // Check casting
      assert !fqn.equals("X");
      // Check null
      assert !fqn.equals(null);
   }

   public void testNullElements() throws CloneNotSupportedException {
      Fqn fqn0 = Fqn.fromElements((Object) null);
      assert 1 == fqn0.size();

      Fqn fqn1 = Fqn.fromElements("NULL", null, 0);
      assert 3 == fqn1.size();

      Fqn fqn2 = Fqn.fromElements("NULL", null, 0);
      assert fqn1.hashCode() == fqn2.hashCode();
      assert fqn1.equals(fqn2);
   }

   public void testIteration() {
      Fqn fqn = Fqn.fromString("/a/b/c");
      assert 3 == fqn.size();
      Fqn tmp_fqn = Fqn.ROOT;
      assert 0 == tmp_fqn.size();
      for (int i = 0; i < fqn.size(); i++) {
         String s = (String) fqn.get(i);
         tmp_fqn = Fqn.fromRelativeElements(tmp_fqn, s);
         assert tmp_fqn.size() == i + 1;
      }
      assert 3 == tmp_fqn.size();
      assert fqn.equals(tmp_fqn);
   }

   public void testIsChildOf() {
      Fqn child = Fqn.fromString("/a/b");
      Fqn parent = Fqn.fromString("/a");
      assert child.isChildOf(parent);
      assert !parent.isChildOf(child);
      assert child.isChildOrEquals(child);

      parent = Fqn.fromString("/a/b/c");
      child = Fqn.fromString("/a/b/c/d/e/f/g/h/e/r/e/r/t/tt/");
      assert child.isChildOf(parent);
   }

   public void testIsChildOf2() {
      Fqn child = Fqn.fromString("/a/b/c/d");
      assert "/b/c/d".equals(child.getSubFqn(1, child.size()).toString());
   }

   public void testParentage() {
      Fqn fqnRoot = Fqn.ROOT;
      Fqn parent = fqnRoot.getParent();
      assert parent.equals(fqnRoot);

      Fqn fqnOne = Fqn.fromString("/one");
      parent = fqnOne.getParent();
      assert parent.equals(fqnRoot);
      assert fqnOne.isChildOf(parent);

      Fqn fqnTwo = Fqn.fromString("/one/two");
      parent = fqnTwo.getParent();
      assert parent.equals(fqnOne);
      assert fqnTwo.isChildOf(parent);

      Fqn fqnThree = Fqn.fromString("/one/two/three");
      parent = fqnThree.getParent();
      assert parent.equals(fqnTwo);
      assert fqnThree.isChildOf(parent);

   }

   public void testRoot() {
      Fqn fqn = Fqn.ROOT;
      assert fqn.isRoot();

      fqn = Fqn.fromString("/one/two");
      assert !fqn.isRoot();

      Fqn f = Fqn.fromString("/");

      assert f.isRoot();
      assert f.equals(Fqn.ROOT);
   }

   public void testGetName() {
      Fqn integerFqn = Fqn.fromElements(1);
      assert "1".equals(integerFqn.getLastElementAsString());

      Object object = new Object();
      Fqn objectFqn = Fqn.fromElements(object);
      assert object.toString().equals(objectFqn.getLastElementAsString());
   }

   // testing generics

   public void testSize() {
      Fqn f = Fqn.ROOT;
      assert f.size() == 0;
      assert f.isRoot();

      f = Fqn.fromString("/");
      assert f.size() == 0;
      assert f.isRoot();

      f = Fqn.fromString("/hello");
      assert f.size() == 1;
      assert !f.isRoot();
   }

   public void testGenerations() {
      Fqn f = Fqn.fromElements(1, 2, 3, 4, 5, 6, 7);

      assert f.equals(f.getAncestor(f.size()));
      assert f.getParent().equals(f.getAncestor(f.size() - 1));
      assert Fqn.ROOT.equals(f.getAncestor(0));
      assert Fqn.fromElements(1).equals(f.getAncestor(1));
      assert Fqn.fromElements(1, 2).equals(f.getAncestor(2));
      assert Fqn.fromElements(1, 2, 3).equals(f.getAncestor(3));
      assert Fqn.fromElements(1, 2, 3, 4).equals(f.getAncestor(4));
      assert Fqn.fromElements(1, 2, 3, 4, 5).equals(f.getAncestor(5));

      try {
         f.getAncestor(-1);
         // should fail
         assert false;
      }
      catch (IllegalArgumentException good) {
         // expected
      }

      try {
         f.getAncestor(f.size() + 1);
         // should fail
         assert false;
      }
      catch (IndexOutOfBoundsException good) {
         // expected
      }
   }

   public void testReplacingDirectAncestor() {
      Fqn fqn = Fqn.fromString("/a/b/c");
      Fqn newParent = Fqn.fromString("/hot/dog");
      Fqn expectedNewChild = Fqn.fromString("/hot/dog/c");

      assert expectedNewChild.equals(fqn.replaceAncestor(fqn.getParent(), newParent));
   }

   public void testReplacingindirectAncestor() {
      Fqn fqn = Fqn.fromString("/a/b/c");
      Fqn newParent = Fqn.fromString("/hot/dog");
      Fqn expectedNewChild = Fqn.fromString("/hot/dog/b/c");

      Fqn replaced = fqn.replaceAncestor(fqn.getParent().getParent(), newParent);
      assert expectedNewChild.equals(replaced) : "Expected " + expectedNewChild + " but was " + replaced;
   }

   public void testDifferentFactories() {
      Fqn[] fqns = new Fqn[6];
      int i = 0;
      fqns[i++] = Fqn.fromString("/a/b/c");
      fqns[i++] = Fqn.fromRelativeElements(Fqn.ROOT, "a", "b", "c");
      fqns[i++] = Fqn.fromElements("a", "b", "c");
      fqns[i++] = Fqn.fromList(Arrays.asList("a", "b", "c"));
      fqns[i++] = Fqn.fromRelativeList(Fqn.ROOT, Arrays.asList("a", "b", "c"));
      fqns[i] = Fqn.fromRelativeFqn(Fqn.ROOT, Fqn.fromString("/a/b/c"));

      // all of the above should be equal to each other.
      for (i = 0; i < fqns.length; i++) {
         for (int j = 0; j < fqns.length; j++) {
            assert fqns[i].equals(fqns[j]) : "Error on equals comparing " + i + " and " + j + ".  i = " + fqns[i] + " and j = " + fqns[j];
            assert fqns[j].equals(fqns[i]) : "Error on equals comparing " + i + " and " + j + ".  i = " + fqns[i] + " and j = " + fqns[j];
            assert fqns[i].hashCode() == fqns[j].hashCode() : "Error on hashcode comparing " + i + " and " + j;
         }
      }
   }
}
