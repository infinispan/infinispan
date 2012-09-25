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

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<BackupConfiguration> {

   private String site;

   private BackupConfiguration.BackupStrategy strategy = BackupConfiguration.BackupStrategy.ASYNC;

   private long replicationTimeout = 10000;

   private BackupFailurePolicy backupFailurePolicy = BackupFailurePolicy.WARN;

   private String failurePolicyClass;

   private TakeOfflineConfigurationBuilder takeOfflineBuilder;

   public BackupConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      takeOfflineBuilder = new TakeOfflineConfigurationBuilder(builder, this);
   }

   /**
    * @param site The name of the site where this cache backups. Must be a valid name, i.e. a site defined in the
    *             global config.
    */
   public BackupConfigurationBuilder site(String site) {
      this.site = site;
      return this;
   }

   /**
    * @see #site(String)
    */
   public String site() {
      return this.site;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return failurePolicyClass;
   }

   /**
    * @see #failurePolicyClass()
    */
   public BackupConfigurationBuilder failurePolicyClass(String failurePolicy) {
      this.failurePolicyClass = failurePolicy;
      return this;
   }

   /**
    * Timeout(millis) used for replicating calls to other sites.
    */
   public BackupConfigurationBuilder replicationTimeout(long replicationTimeout) {
      this.replicationTimeout = replicationTimeout;
      return this;
   }

   /**
    * @see {@link #replicationTimeout(long)}
    */
   public long replicationTimeout() {
      return replicationTimeout;
   }

   /**
    * Sets the strategy used for backing up data: sync or async. If not specified defaults
    * to {@link BackupConfiguration.BackupStrategy.ASYNC}.
    */
   public BackupConfigurationBuilder strategy(BackupConfiguration.BackupStrategy strategy) {
      this.strategy = strategy;
      return this;
   }

   /**
    * @see #strategy()
    */
   public BackupConfiguration.BackupStrategy strategy() {
      return strategy;
   }

   public TakeOfflineConfigurationBuilder takeOffline() {
      return takeOfflineBuilder;
   }

   /**
    * Configures how the system behaves when the backup call fails. Only applies to sync backus.
    * The default values is  {@link BackupFailurePolicy.WARN}
    */
   public BackupConfigurationBuilder backupFailurePolicy(BackupFailurePolicy backupFailurePolicy) {
      this.backupFailurePolicy = backupFailurePolicy;
      return this;
   }

   /**
    * @see {@link #backupFailurePolicy(BackupFailurePolicy backupFailurePolicy)}
    */
   public BackupFailurePolicy backupFailurePolicy() {
      return this.backupFailurePolicy;
   }



   @Override
   public void validate() {
      takeOfflineBuilder.validate();
      if (site == null)
         throw new ConfigurationException("The 'site' must be specified!");
      if (backupFailurePolicy == BackupFailurePolicy.CUSTOM && (failurePolicyClass == null)) {
         throw new ConfigurationException("It is required to specify a 'failurePolicyClass' when using a " +
                                                "custom backup failure policy!");
      }
   }

   @Override
   public BackupConfiguration create() {
      return new BackupConfiguration(site, strategy, replicationTimeout, backupFailurePolicy, failurePolicyClass,
                                     takeOfflineBuilder.create());
   }

   @Override
   public Builder read(BackupConfiguration template) {
      this.takeOfflineBuilder.read(template.takeOffline());
      this.site = template.site();
      this.strategy = template.strategy();
      this.backupFailurePolicy = template.backupFailurePolicy();
      this.replicationTimeout = template.replicationTimeout();
      this.failurePolicyClass = template.failurePolicyClass();
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupConfigurationBuilder)) return false;

      BackupConfigurationBuilder that = (BackupConfigurationBuilder) o;

      if (replicationTimeout != that.replicationTimeout) return false;
      if (backupFailurePolicy != that.backupFailurePolicy) return false;
      if (failurePolicyClass != null ? !failurePolicyClass.equals(that.failurePolicyClass) : that.failurePolicyClass != null)
         return false;
      if (site != null ? !site.equals(that.site) : that.site != null) return false;
      if (strategy != that.strategy) return false;
      if (takeOfflineBuilder != null ? !takeOfflineBuilder.equals(that.takeOfflineBuilder) : that.takeOfflineBuilder != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = site != null ? site.hashCode() : 0;
      result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
      result = 31 * result + (int) (replicationTimeout ^ (replicationTimeout >>> 32));
      result = 31 * result + (backupFailurePolicy != null ? backupFailurePolicy.hashCode() : 0);
      result = 31 * result + (failurePolicyClass != null ? failurePolicyClass.hashCode() : 0);
      result = 31 * result + (takeOfflineBuilder != null ? takeOfflineBuilder.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "BackupConfigurationBuilder{" +
            "site='" + site + '\'' +
            ", strategy=" + strategy +
            ", replicationTimeout=" + replicationTimeout +
            ", backupFailurePolicy=" + backupFailurePolicy +
            ", failurePolicyClass='" + failurePolicyClass + '\'' +
            ", takeOfflineBuilder=" + takeOfflineBuilder +
            '}';
   }
}
