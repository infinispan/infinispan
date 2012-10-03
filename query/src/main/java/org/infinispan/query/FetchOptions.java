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

   /**
    * Set the fetch mode to be used to fetch matching results
    * @param fetchMode
    * @return {@code this} to allow method chaining
    */
   public FetchOptions fetchMode(FetchMode fetchMode) {
      if (fetchMode == null) {
         throw new IllegalArgumentException("fetchMode should not be null");
      }
      this.fetchMode = fetchMode;
      return this;
   }

   /**
    * Set the fetch size for batch loading of matches
    * @param fetchSize
    * @return {@code this} to allow method chaining
    */
   public FetchOptions fetchSize(int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("fetchSize should be greater than 0");
      }
      this.fetchSize = fetchSize;
      return this;
   }

   /**
    * @return the selected fetch mode
    */
   public FetchMode getFetchMode() {
      return fetchMode;
   }

   /**
    * @return the used fetch size
    */
   public int getFetchSize() {
      return fetchSize;
   }

   /**
    * Specifies the fetching strategy
    * for query results.
    */
    public static enum FetchMode {

        /**
         * With eager mode all results are loaded as
         * soon as the query is performed; this results
         * in a larger initial transfer of entries but no
         * remote operations during iteration of the resultset.
         */
        EAGER,

        /**
         * With lazy loading the entries are not loaded
         * until each one is specifically requested.
         * If iterating on very larger result sets this
         * is recommended to avoid loading too many entries
         * in the VM performing the iteration.
         */
        LAZY
    }
}
