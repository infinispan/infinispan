package org.infinispan.rest.framework.openapi;

public enum ParameterIn {
   QUERY, HEADER, PATH, COOKIE;

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
