package org.infinispan.server.core.dataconversion.json;

import java.util.Collection;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
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
