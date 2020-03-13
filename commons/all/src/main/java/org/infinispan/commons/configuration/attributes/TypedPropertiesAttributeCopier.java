package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.util.TypedProperties;

/**
 * TypedPropertiesAttributeCopier. This {@link AttributeCopier} can handle {@link TypedProperties}
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class TypedPropertiesAttributeCopier implements AttributeCopier<TypedProperties> {
   public static final AttributeCopier<TypedProperties> INSTANCE = new TypedPropertiesAttributeCopier();
   @Override
   public TypedProperties copyAttribute(TypedProperties attribute) {
      return new TypedProperties(attribute);
   }
}
