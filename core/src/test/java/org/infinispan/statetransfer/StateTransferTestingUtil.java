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
package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.test.TestingUtil;

/**
 * StateTransferTestingUtil.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class StateTransferTestingUtil {
   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_D_NAME = "a_d_age";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String A_D_AGE = "a_d_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final String JANE = "JANE";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;

   public static void verifyNoDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheLoader();
      assert !l.containsKey(A_B_AGE);
      assert !l.containsKey(A_B_NAME);
      assert !l.containsKey(A_C_AGE);
      assert !l.containsKey(A_C_NAME);
      assert !l.containsKey(A_D_AGE);
      assert !l.containsKey(A_D_NAME);
   }

   public static void verifyNoData(Cache<Object, Object> c) {
      assert c.isEmpty() : "Cache should be empty!";
   }

   public static void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
      c.evict(A_B_NAME);
      c.evict(A_B_AGE);
      c.evict(A_C_NAME);
      c.evict(A_C_AGE);
      c.evict(A_D_NAME);
      c.evict(A_D_AGE);
   }

   public static void verifyInitialDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheLoader();
      assert l.containsKey(A_B_AGE);
      assert l.containsKey(A_B_NAME);
      assert l.containsKey(A_C_AGE);
      assert l.containsKey(A_C_NAME);
      assert l.load(A_B_AGE).getValue().equals(TWENTY);
      assert l.load(A_B_NAME).getValue().equals(JOE);
      assert l.load(A_C_AGE).getValue().equals(FORTY);
      assert l.load(A_C_NAME).getValue().equals(BOB);
   }

   public static void verifyInitialData(Cache<Object, Object> c) {
      assert JOE.equals(c.get(A_B_NAME)) : "Incorrect value for key " + A_B_NAME;
      assert TWENTY.equals(c.get(A_B_AGE)) : "Incorrect value for key " + A_B_AGE;
      assert BOB.equals(c.get(A_C_NAME)) : "Incorrect value for key " + A_C_NAME;
      assert FORTY.equals(c.get(A_C_AGE)) : "Incorrect value for key " + A_C_AGE;
   }
}
