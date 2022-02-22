package org.infinispan.plugins.maven.defaults;

import java.util.Map;
import java.util.Set;

/**
 * Interface used by {@link org.infinispan.plugins.maven.defaults.DefaultsExtractorMojo} to extract default default
 * values from AttributeDefinitions.
 *
 * @author Ryan Emerson
 */
interface DefaultsResolver {
   boolean isValidClass(String className);
   Map<String, String> extractDefaults(Set<Class> classes, String separator);
}
