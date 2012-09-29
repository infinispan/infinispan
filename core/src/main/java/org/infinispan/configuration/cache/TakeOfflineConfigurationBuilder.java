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

import org.infinispan.configuration.Builder;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfigurationBuilder extends AbstractConfigurationChildBuilder<TakeOfflineConfiguration> {

   private int afterFailures = 0;
   private long minTimeToWait = 0;
   private BackupConfigurationBuilder backupConfigurationBuilder;

   public TakeOfflineConfigurationBuilder(ConfigurationBuilder builder, BackupConfigurationBuilder backupConfigurationBuilder) {
      super(builder);
      this.backupConfigurationBuilder = backupConfigurationBuilder;
   }

   @Override
   public void validate() {
   }

   @Override
   public TakeOfflineConfiguration create() {
      return new TakeOfflineConfiguration(afterFailures, minTimeToWait);
   }

   @Override
   public Builder<?> read(TakeOfflineConfiguration template) {
      this.afterFailures = template.afterFailures();
      this.minTimeToWait = template.minTimeToWait();
      return this;
   }

   /**
    * The minimal number of millis to wait before taking this site offline, even in the case 'afterFailures' is reached.
    * If smaller or equal to 0, then only 'afterFailures' is considered.
    */
   public TakeOfflineConfigurationBuilder minTimeToWait(long minTimeToWait) {
      this.minTimeToWait = minTimeToWait;
      return this;
   }

   /**
    * The number of failed request operations after which this site should be taken offline. Defaults to 0 (never). A
    * negative value would mean that the site will be taken offline after 'minTimeToWait'.
    */
   public TakeOfflineConfigurationBuilder afterFailures(int afterFailures) {
      this.afterFailures = afterFailures;
      return this;
   }

   public BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TakeOfflineConfigurationBuilder)) return false;

      TakeOfflineConfigurationBuilder that = (TakeOfflineConfigurationBuilder) o;

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
      return "TakeOfflineConfigurationBuilder{" +
            "afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            '}';
   }
}
