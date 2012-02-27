package org.infinispan.configuration.cache;

public class InvocationBatchingConfiguration {

   private final boolean enabled;

   InvocationBatchingConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfiguration{" +
            "enabled=" + enabled +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InvocationBatchingConfiguration that = (InvocationBatchingConfiguration) o;

      if (enabled != that.enabled) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return (enabled ? 1 : 0);
   }

}
