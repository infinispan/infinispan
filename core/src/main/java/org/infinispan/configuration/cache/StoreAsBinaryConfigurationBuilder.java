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

/**
 * Controls whether when stored in memory, keys and values are stored as references to their original objects, or in
 * a serialized, binary format.  There are benefits to both approaches, but often if used in a clustered mode,
 * storing objects as binary means that the cost of serialization happens early on, and can be amortized.  Further,
 * deserialization costs are incurred lazily which improves throughput.
 * <p />
 * It is possible to control this on a fine-grained basis: you can choose to just store keys or values as binary, or
 * both.
 * <p />
 * @see StoreAsBinaryConfiguration
 */
public class StoreAsBinaryConfigurationBuilder extends AbstractConfigurationChildBuilder<StoreAsBinaryConfiguration> {

   private boolean enabled = false;
   private boolean storeKeysAsBinary = true;
   private boolean storeValuesAsBinary = true;
   
   StoreAsBinaryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enables storing both keys and values as binary.
    */
   public StoreAsBinaryConfigurationBuilder enable() {
      enabled = true;
      return this;
   }

   /**
    * Disables storing both keys and values as binary.
    */
   public StoreAsBinaryConfigurationBuilder disable() {
      enabled = false;
      return this;
   }

   /**
    * Sets whether this feature is enabled or disabled.
    * @param enabled if true, this feature is enabled.  If false, it is disabled.
    */
   public StoreAsBinaryConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Specify whether keys are stored as binary or not.
    * @param storeKeysAsBinary if true, keys are stored as binary.  If false, keys are stored as object references.
    */
   public StoreAsBinaryConfigurationBuilder storeKeysAsBinary(boolean storeKeysAsBinary) {
      this.storeKeysAsBinary = storeKeysAsBinary;
      return this;
   }
   /**
    * Specify whether values are stored as binary or not.
    * @param storeValuesAsBinary if true, values are stored as binary.  If false, values are stored as object references.
    */
   public StoreAsBinaryConfigurationBuilder storeValuesAsBinary(boolean storeValuesAsBinary) {
      this.storeValuesAsBinary = storeValuesAsBinary;
      return this;
   }

   @Override
   void validate() {
      // Nothing to validate.
   }

   @Override
   StoreAsBinaryConfiguration create() {
      return new StoreAsBinaryConfiguration(enabled, storeKeysAsBinary, storeValuesAsBinary);
   }   
   
   @Override
   public StoreAsBinaryConfigurationBuilder read(StoreAsBinaryConfiguration template) {
      this.enabled = template.enabled();
      this.storeKeysAsBinary = template.storeKeysAsBinary();
      this.storeValuesAsBinary = template.storeValuesAsBinary();
      
      return this;
   }

   @Override
   public String toString() {
      return "StoreAsBinaryConfigurationBuilder{" +
            "enabled=" + enabled +
            ", storeKeysAsBinary=" + storeKeysAsBinary +
            ", storeValuesAsBinary=" + storeValuesAsBinary +
            '}';
   }

}
