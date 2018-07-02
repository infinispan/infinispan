package org.infinispan.server.core.dataconversion.json;

import java.util.Collection;

import org.codehaus.jackson.map.MapperConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;
import org.codehaus.jackson.map.jsontype.TypeIdResolver;
import org.codehaus.jackson.type.JavaType;
import org.infinispan.commons.configuration.ClassWhiteList;

/**
 * Builder that can produce {@link SecureTypeIdResolver} from an existing TypeIdResolver.
 *
 * @since 9.3
 */
public class SecureTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

   private final ClassWhiteList whiteList;

   protected SecureTypeResolverBuilder(ObjectMapper.DefaultTyping defaultTyping, ClassWhiteList whiteList) {
      super(defaultTyping);
      this.whiteList = whiteList;
   }

   protected TypeIdResolver idResolver(MapperConfig<?> config, JavaType baseType,
                                       Collection<NamedType> subtypes, boolean forSer, boolean forDeser) {
      TypeIdResolver result = super.idResolver(config, baseType, subtypes, forSer, forDeser);
      return new SecureTypeIdResolver(result, whiteList);
   }
}
