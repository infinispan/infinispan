/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;

public class InvocationBatchingConfigurationBuilder extends AbstractConfigurationChildBuilder<InvocationBatchingConfiguration> {

   boolean enabled = false;
   
   InvocationBatchingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public InvocationBatchingConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public InvocationBatchingConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public InvocationBatchingConfigurationBuilder enable(boolean enable) {
      this.enabled = enable;
      return this;
   }

   @Override
   void validate() {
      if (enabled && getBuilder().transaction().transactionMode != null && getBuilder().transaction().transactionMode.equals(NON_TRANSACTIONAL))
         throw new IllegalStateException("Cannot enable Invocation Batching when the Transaction Mode is NON_TRANSACTIONAL, set the transaction mode to TRANSACTIONAL");
   }

   @Override
   InvocationBatchingConfiguration create() {
      return new InvocationBatchingConfiguration(enabled);
   }
   
   @Override
   public InvocationBatchingConfigurationBuilder read(InvocationBatchingConfiguration template) {
      this.enabled = template.enabled();
      
      return this;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfigurationBuilder{" +
            "enabled=" + enabled +
            '}';
   }

}
