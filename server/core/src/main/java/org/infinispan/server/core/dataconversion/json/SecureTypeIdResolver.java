package org.infinispan.server.core.dataconversion.json;

import java.io.IOException;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

/**
 * Jackson TypeIdResolver that checks the serialization allow list before deserializing JSON types.
 *
 * @since 9.3
 *
 * @deprecated JSON to POJO conversion is deprecated and will be removed in a future version.
 */
@Deprecated(forRemoval=true, since = "12.0")
public class SecureTypeIdResolver implements TypeIdResolver {

   protected static final Log logger = LogFactory.getLog(SecureTypeIdResolver.class, Log.class);

   private final TypeIdResolver internalTypeIdResolver;
   private final ClassAllowList classAllowList;

   SecureTypeIdResolver(TypeIdResolver typeIdResolver, ClassAllowList classAllowList) {
      this.internalTypeIdResolver = typeIdResolver;
      this.classAllowList = classAllowList;
   }

   @Override
   public void init(JavaType baseType) {
      internalTypeIdResolver.init(baseType);
   }

   @Override
   public String idFromValue(Object value) {
      return internalTypeIdResolver.idFromValue(value);
   }

   @Override
   public String idFromValueAndType(Object value, Class<?> suggestedType) {
      return internalTypeIdResolver.idFromValueAndType(value, suggestedType);
   }

   @Override
   public String idFromBaseType() {
      return internalTypeIdResolver.idFromBaseType();
   }

   @Override
   public String getDescForKnownTypeIds() {
      return internalTypeIdResolver.getDescForKnownTypeIds();
   }

   @Override
   public JavaType typeFromId(DatabindContext databindContext, String id) throws IOException {
      JavaType javaType = internalTypeIdResolver.typeFromId(databindContext, id);
      Class<?> clazz = javaType.getRawClass();
      String className = clazz.getName();
      if (!classAllowList.isSafeClass(className)) {
         throw logger.errorDeserializing(className);
      }
      return javaType;
   }

   @Override
   public JsonTypeInfo.Id getMechanism() {
      return internalTypeIdResolver.getMechanism();
   }
}
