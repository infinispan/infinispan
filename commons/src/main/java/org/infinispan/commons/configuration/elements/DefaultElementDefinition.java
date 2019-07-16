package org.infinispan.commons.configuration.elements;

import org.infinispan.commons.configuration.ConfigurationInfo;

public class DefaultElementDefinition<C extends ConfigurationInfo> implements ElementDefinition<C> {

   private final String name;
   private final boolean isTopLevel;
   private final boolean omitIfEmpty;

   public DefaultElementDefinition(String name, boolean isTopLevel) {
      this(name, isTopLevel, true);
   }

   public DefaultElementDefinition(String name, boolean isTopLevel, boolean omitIfEmpty) {
      this.name = name;
      this.isTopLevel = isTopLevel;
      this.omitIfEmpty = omitIfEmpty;
   }

   public DefaultElementDefinition(String name) {
      this(name, true, true);
   }

   @Override
   public boolean omitIfEmpty() {
      return omitIfEmpty;
   }

   @Override
   public boolean isTopLevel() {
      return isTopLevel;
   }

   @Override
   public ElementOutput toExternalName(C configuration) {
      return new ElementOutput(name);
   }

   @Override
   public boolean supports(String serializedName) {
      return name != null && name.equals(serializedName);
   }
}
