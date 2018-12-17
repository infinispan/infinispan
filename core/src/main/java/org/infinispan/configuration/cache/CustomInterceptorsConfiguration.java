package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.CUSTOM_INTERCEPTORS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Configures custom interceptors to be added to the cache.
 *
 * @author pmuir
 */
public class CustomInterceptorsConfiguration implements Matchable<CustomInterceptorsConfiguration>, ConfigurationInfo {

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CUSTOM_INTERCEPTORS.getLocalName());
   private List<InterceptorConfiguration> interceptors;
   private List<ConfigurationInfo> subElement;

   CustomInterceptorsConfiguration(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
      subElement = new ArrayList<>(interceptors.size());
      subElement.addAll(interceptors);
   }

   public CustomInterceptorsConfiguration() {
      this(Collections.emptyList());
   }

   /**
    * This specifies a list of {@link InterceptorConfiguration} instances to be referenced when building the interceptor
    * chain.
    * @return A list of {@link InterceptorConfiguration}s. May be an empty list, will never be null.
    */
   public List<InterceptorConfiguration> interceptors() {
      return interceptors;
   }

   public CustomInterceptorsConfiguration interceptors(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
      return this;
   }

   @Override
   public String toString() {
      return "CustomInterceptorsConfiguration [interceptors=" + interceptors + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomInterceptorsConfiguration that = (CustomInterceptorsConfiguration) o;

      if (interceptors != null ? !interceptors.equals(that.interceptors) : that.interceptors != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return interceptors != null ? interceptors.hashCode() : 0;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElement;
   }
}
