package org.infinispan.server.core.dataconversion.json;

import java.util.Collection;

import org.infinispan.commons.configuration.ClassAllowList;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;

/**
 * Builder that can produce {@link SecureTypeIdResolver} from an existing TypeIdResolver.
 *
 * @since 9.3
 *
 * @deprecated JSON to POJO conversion is deprecated and will be removed in a future version.
 */
@Deprecated
public class SecureTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

   private final ClassAllowList allowList;

   protected SecureTypeResolverBuilder(ObjectMapper.DefaultTyping defaultTyping, ClassAllowList allowList) {
      super(defaultTyping, LaissezFaireSubTypeValidator.instance);
      this.allowList = allowList;
   }

   protected TypeIdResolver idResolver(MapperConfig<?> config,
                                       JavaType baseType, PolymorphicTypeValidator subtypeValidator,
                                       Collection<NamedType> subtypes, boolean forSer, boolean forDeser) {
      TypeIdResolver result = super.idResolver(config, baseType, subtypeValidator, subtypes, forSer, forDeser);
      return new SecureTypeIdResolver(result, allowList);
   }
}
