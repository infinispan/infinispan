package org.infinispan.stack;

import javassist.CtMethod;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Caller {
   public final CtMethod method;
   public final int pc;

   public Caller(CtMethod method, int pc) {
      this.method = method;
      this.pc = pc;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Caller caller = (Caller) o;

      if (pc != caller.pc) return false;
      return method.equals(caller.method);

   }

   @Override
   public int hashCode() {
      int result = method.hashCode();
      result = 31 * result + pc;
      return result;
   }
}
