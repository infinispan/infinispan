package org.infinispan.server.core.dataconversion.json;

import java.io.IOException;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

/**
 * Jackson TypeIdResolver that checks the serialization whitelist before deserializing JSON types.
 *
 * @since 9.3
 */
public class SecureTypeIdResolver implements TypeIdResolver {

   protected final static Log logger = LogFactory.getLog(SecureTypeIdResolver.class, Log.class);

   private TypeIdResolver internalTypeIdResolver;
   private final ClassWhiteList classWhiteList;

   SecureTypeIdResolver(TypeIdResolver typeIdResolver, ClassWhiteList classWhiteList) {
      this.internalTypeIdResolver = typeIdResolver;
      this.classWhiteList = classWhiteList;
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
   public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      JavaType javaType = internalTypeIdResolver.typeFromId(context, id);
      Class<?> clazz = javaType.getRawClass();
      String className = clazz.getName();
      if (!classWhiteList.isSafeClass(className)) {
         throw logger.errorDeserializing(className);
      }
      return javaType;
   }

   @Override
   public String getDescForKnownTypeIds() {
      return internalTypeIdResolver.getDescForKnownTypeIds();
   }


   @Override
   public JsonTypeInfo.Id getMechanism() {
      return internalTypeIdResolver.getMechanism();
   }
}
