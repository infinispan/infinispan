package org.infinispan.stack;

import javassist.CtMethod;

/**
 * CtMethod does not use declaring class for comparison
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class SpecificMethod {
   public final CtMethod method;

   public SpecificMethod(CtMethod method) {
      this.method = method;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SpecificMethod other = (SpecificMethod) o;
      return method.getName().equals(other.method.getName())
            && method.getSignature().equals(other.method.getSignature())
            && method.getDeclaringClass().equals(other.method.getDeclaringClass());

   }

   @Override
   public int hashCode() {
      return 31 * method.hashCode() + method.getDeclaringClass().hashCode();
   }

   @Override
   public String toString() {
      return method.getLongName();
   }
}
