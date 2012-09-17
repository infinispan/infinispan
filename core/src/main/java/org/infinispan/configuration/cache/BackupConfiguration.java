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
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfiguration {

   private final String site;
   private final BackupStrategy strategy;
   private long timeout;
   private final BackupFailurePolicy backupFailurePolicy;
   private final String failurePolicyClass;

   public BackupConfiguration(String site, BackupStrategy strategy, long timeout, BackupFailurePolicy backupFailurePolicy, String failurePolicyClass) {
      this.site = site;
      this.strategy = strategy;
      this.timeout = timeout;
      this.backupFailurePolicy = backupFailurePolicy;
      this.failurePolicyClass = failurePolicyClass;
   }

   /**
    * Returns the name of the site where this cache backups its data.
    */
   public String site() {
      return site;
   }

   /**
    * How does the backup happen: sync or async.
    */
   public BackupStrategy strategy() {
      return strategy;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return failurePolicyClass;
   }

   public boolean isAsyncBackup() {
      return strategy() == BackupStrategy.ASYNC;
   }

   public long replicationTimeout() {
      return timeout;
   }

   public BackupFailurePolicy backupFailurePolicy() {
      return backupFailurePolicy;
   }

   public enum BackupStrategy {
      SYNC, ASYNC
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupConfiguration)) return false;

      BackupConfiguration that = (BackupConfiguration) o;

      if (timeout != that.timeout) return false;
      if (backupFailurePolicy != that.backupFailurePolicy) return false;
      if (failurePolicyClass != null ? !failurePolicyClass.equals(that.failurePolicyClass) : that.failurePolicyClass != null)
         return false;
      if (site != null ? !site.equals(that.site) : that.site != null) return false;
      if (strategy != that.strategy) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = site != null ? site.hashCode() : 0;
      result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + (backupFailurePolicy != null ? backupFailurePolicy.hashCode() : 0);
      result = 31 * result + (failurePolicyClass != null ? failurePolicyClass.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "BackupConfiguration{" +
            "site='" + site + '\'' +
            ", strategy=" + strategy +
            ", timeout=" + timeout +
            ", backupFailurePolicy=" + backupFailurePolicy +
            ", failurePolicyClass='" + failurePolicyClass + '\'' +
            '}';
   }
}
