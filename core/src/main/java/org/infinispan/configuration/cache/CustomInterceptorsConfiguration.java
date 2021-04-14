package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;

/**
 * Configures custom interceptors to be added to the cache.
 *
 * @author pmuir
 * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
 */
@Deprecated
public class CustomInterceptorsConfiguration implements Matchable<CustomInterceptorsConfiguration> {

   private List<InterceptorConfiguration> interceptors;

   CustomInterceptorsConfiguration(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
   }

   CustomInterceptorsConfiguration() {
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

   public AttributeSet attributes() {
      return null;
   }
}
