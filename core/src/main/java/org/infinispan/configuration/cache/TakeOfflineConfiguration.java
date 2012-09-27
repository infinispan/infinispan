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

package org.infinispan.configuration.cache;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfiguration {

   private int afterFailures;
   private long minTimeToWait;

   public TakeOfflineConfiguration(int afterFailures, long minTimeToWait) {
      this.afterFailures = afterFailures;
      this.minTimeToWait = minTimeToWait;
   }

   /**
    * @see TakeOfflineConfigurationBuilder#afterFailures(int)
    */
   public int afterFailures() {
      return afterFailures;
   }

   /**
    * @see TakeOfflineConfigurationBuilder#minTimeToWait(long)
    */
   public long minTimeToWait() {
      return minTimeToWait;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TakeOfflineConfiguration)) return false;

      TakeOfflineConfiguration that = (TakeOfflineConfiguration) o;

      if (afterFailures != that.afterFailures) return false;
      if (minTimeToWait != that.minTimeToWait) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = afterFailures;
      result = 31 * result + (int) (minTimeToWait ^ (minTimeToWait >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "TakeOfflineConfiguration{" +
            "afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            '}';
   }
}
