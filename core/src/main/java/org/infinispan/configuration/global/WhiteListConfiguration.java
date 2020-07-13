package org.infinispan.configuration.global;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

@Deprecated
@BuiltBy(WhiteListConfigurationBuilder.class)
public class WhiteListConfiguration implements ConfigurationInfo {

   AllowListConfiguration delegate;


   WhiteListConfiguration(AllowListConfiguration delegate) {
      this.delegate = delegate;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return AllowListConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return delegate.attributes();
   }

   public ClassAllowList create() {
      return delegate.create();
   }

   public Set<String> getClasses() {
      return delegate.getClasses();
   }

   public List<String> getRegexps() {
      return delegate.getRegexps();
   }

   @Override
   public boolean equals(Object o) {

      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WhiteListConfiguration that = (WhiteListConfiguration) o;
      return Objects.equals(delegate.attributes(), that.delegate.attributes());
   }

   @Override
   public int hashCode() {
      return delegate.hashCode();
   }

   @Override
   public String toString() {
      return delegate.toString();
   }
}
