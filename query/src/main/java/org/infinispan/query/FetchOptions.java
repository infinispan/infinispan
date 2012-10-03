/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.query;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class FetchOptions {

   private FetchMode fetchMode;

   private int fetchSize;

   public FetchOptions() {
      fetchMode(FetchMode.LAZY);
      fetchSize(1);
   }

   public FetchOptions fetchMode(FetchMode fetchMode) {
      if (fetchMode == null) {
         throw new IllegalArgumentException("fetchMode should not be null");
      }
      this.fetchMode = fetchMode;
      return this;
   }

   public FetchOptions fetchSize(int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("fetchSize should be greater than 0");
      }
      this.fetchSize = fetchSize;
      return this;
   }

   public FetchMode getFetchMode() {
      return fetchMode;
   }

   public int getFetchSize() {
      return fetchSize;
   }

   public static enum FetchMode {
      EAGER, LAZY
   }
}
