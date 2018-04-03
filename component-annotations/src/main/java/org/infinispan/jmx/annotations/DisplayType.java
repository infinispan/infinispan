package org.infinispan.jmx.annotations;

public enum DisplayType {
   SUMMARY, DETAIL;

   @Override
   public String toString() {
      return this.name().toLowerCase();
   }

}
