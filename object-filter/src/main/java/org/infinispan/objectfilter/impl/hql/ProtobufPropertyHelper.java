package org.infinispan.objectfilter.impl.hql;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.protostream.SerializationContext;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufPropertyHelper extends ObjectPropertyHelper<Descriptors.Descriptor> {

   private static final Log log = Logger.getMessageLogger(Log.class, ProtobufPropertyHelper.class.getName());

   private final SerializationContext serializationContext;

   // the EntityNamesResolver of the hql parser is not nicely designed to handle non-Class type metadata
   private final EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
      @Override
      public Class<?> getClassFromName(String entityName) {
         // Here we return a 'fake' class. It does not matter what we return as long as it is non-null
         return serializationContext.canMarshall(entityName) ? Object.class : null;
      }
   };

   private static final Set<Class<?>> primitives = new HashSet<Class<?>>();

   static {
      //todo [anistor] handle arrays and collections
      primitives.add(java.util.Date.class);
      primitives.add(String.class);
      primitives.add(Character.class);
      primitives.add(char.class);
      primitives.add(Double.class);
      primitives.add(double.class);
      primitives.add(Float.class);
      primitives.add(float.class);
      primitives.add(Long.class);
      primitives.add(long.class);
      primitives.add(Integer.class);
      primitives.add(int.class);
      primitives.add(Short.class);
      primitives.add(short.class);
      primitives.add(Byte.class);
      primitives.add(byte.class);
      primitives.add(Boolean.class);
      primitives.add(boolean.class);
   }

   public ProtobufPropertyHelper(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   public EntityNamesResolver getEntityNamesResolver() {
      return entityNamesResolver;
   }

   @Override
   public Descriptors.Descriptor getEntityMetadata(String targetTypeName) {
      return serializationContext.getMessageDescriptor(targetTypeName);
   }

   @Override
   public Class<?> getPropertyType(String entityType, List<String> propertyPath) {
      return getPropertyType(getField(entityType, propertyPath));
   }

   private Descriptors.FieldDescriptor getField(String entityType, List<String> propertyPath) {
      Descriptors.Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      Descriptors.FieldDescriptor field;
      int i = 0;
      for (String p : propertyPath) {
         i++;
         field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            return null;
         }
         if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            return i == propertyPath.size() ? field : null;
         }
      }
      return null;
   }

   private Class<?> getPropertyType(Descriptors.FieldDescriptor field) {
      switch (field.getJavaType()) {
         case INT:
            return Integer.class;
         case LONG:
            return Long.class;
         case FLOAT:
            return Float.class;
         case DOUBLE:
            return Double.class;
         case BOOLEAN:
            return Boolean.class;
         case STRING:
            return String.class;
         case BYTE_STRING:
            return ByteString.class;
         case ENUM:
            return Integer.class;
      }
      return null;
   }

   @Override
   public boolean hasProperty(String entityType, List<String> propertyPath) {
      Descriptors.Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      int i = 0;
      for (String p : propertyPath) {
         i++;
         Descriptors.FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            return false;
         }
         if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            break;
         }
      }
      return i == propertyPath.size();
   }

   @Override
   public boolean hasEmbeddedProperty(String entityType, List<String> propertyPath) {
      Descriptors.Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      for (String p : propertyPath) {
         Descriptors.FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            return false;
         }
         if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public Object convertToPropertyType(String entityType, List<String> propertyPath, String value) {
      Descriptors.FieldDescriptor field = getField(entityType, propertyPath);

      //todo [anistor] this is just for remote query because booleans and enums are handled as integers for historical reasons.
      if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN) {
         try {
            return Integer.parseInt(value) != 0;
         } catch (NumberFormatException e) {
            return Boolean.valueOf(value);
         }
      } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
         Descriptors.EnumDescriptor enumType = field.getEnumType();
         Descriptors.EnumValueDescriptor enumValue;
         try {
            enumValue = enumType.findValueByNumber(Integer.parseInt(value));
         } catch (NumberFormatException e) {
            enumValue = enumType.findValueByName(value);
         }
         if (enumValue == null) {
            throw new IllegalArgumentException("Unknown enum value for enum type " + enumType.getFullName() + " : " + value);
         }
         return enumValue.getNumber();
      }

      return super.convertToPropertyType(entityType, propertyPath, value);
   }
}
