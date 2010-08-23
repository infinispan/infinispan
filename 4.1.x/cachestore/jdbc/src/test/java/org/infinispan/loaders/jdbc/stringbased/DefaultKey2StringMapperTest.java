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
package org.infinispan.loaders.jdbc.stringbased;

import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.loaders.jdbc.stringbased.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "loaders.jdbc.stringbased.DefaultKey2StringMapperTest")
public class DefaultKey2StringMapperTest {

   DefaultKey2StringMapper mapper = new DefaultKey2StringMapper();

   public void testPrimitivesAreSupported() {
      assert mapper.isSupportedType(Integer.class);
      assert mapper.isSupportedType(Byte.class);
      assert mapper.isSupportedType(Short.class);
      assert mapper.isSupportedType(Long.class);
      assert mapper.isSupportedType(Double.class);
      assert mapper.isSupportedType(Float.class);
      assert mapper.isSupportedType(Boolean.class);
      assert mapper.isSupportedType(String.class);
   }

   @SuppressWarnings(value = "all")
   public void testGetStingMapping() {
      Object[] toTest = {0, new Byte("1"), new Short("2"), (long) 3, new Double("3.4"), new Float("3.5"), Boolean.FALSE, "some string"};
      for (Object o : toTest) {
         assert mapper.getStringMapping(o).equals(o.toString());
      }
   }
}
