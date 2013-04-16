/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.spring;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

public class AssertionUtils {
   public static void assertPropertiesSubset(String message, Properties expected, Properties actual) {
      for(String key : expected.stringPropertyNames()) {
         assertTrue(String.format("%s Key %s missing from %s", message, key, actual), actual.containsKey(key));
         assertEquals(String.format("%s Key %s's expected value was \"%s\" but actual was \"%s\"", message, key, expected.getProperty(key), actual.getProperty(key)), expected.getProperty(key), actual.getProperty(key));
      }
   }
}
