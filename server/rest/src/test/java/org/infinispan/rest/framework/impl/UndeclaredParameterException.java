package org.infinispan.rest.framework.impl;

/**
 * Thrown when a REST handler accesses a parameter, header, or request body that is not declared in the OpenAPI schema
 * for that endpoint.
 */
public class UndeclaredParameterException extends RuntimeException {

   public UndeclaredParameterException(String parameterType, String name, String endpoint) {
      super("%s '%s' accessed but not declared in OpenAPI schema for %s".formatted(parameterType, name, endpoint));
   }

   public UndeclaredParameterException(String message) {
      super(message);
   }
}
