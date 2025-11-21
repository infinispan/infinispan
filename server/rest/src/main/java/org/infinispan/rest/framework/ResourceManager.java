package org.infinispan.rest.framework;

/**
 * Handles registration and lookup of {@link ResourceHandler}.
 *
 * @since 10.0
 */
public interface ResourceManager {

   void registerResource(String context, ResourceHandler handler) throws RegistrationException;

   LookupResult lookupResource(Method method, String path, String action);

   default LookupResult lookupResource(Method method, String path) {
      return lookupResource(method, path, null);
   }

   InvocationRegistry registry();
}
