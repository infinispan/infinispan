package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.EnumDescriptor;
import org.infinispan.protostream.descriptors.EnumValueDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufPropertyHelper extends ObjectPropertyHelper<Descriptor> {

   private static final Log log = Logger.getMessageLogger(Log.class, ProtobufPropertyHelper.class.getName());

   private final SerializationContext serializationContext;

   public ProtobufPropertyHelper(EntityNamesResolver entityNamesResolver, SerializationContext serializationContext) {
      super(entityNamesResolver);
      this.serializationContext = serializationContext;
   }

   @Override
   public Descriptor getEntityMetadata(String targetTypeName) {
      return serializationContext.getMessageDescriptor(targetTypeName);
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, List<String> propertyPath) {
      FieldDescriptor field = getField(entityType, propertyPath);
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
            return byte[].class;
         case ENUM:
            return Integer.class;
      }
      return null;
   }

   /**
    * @param entityType
    * @param propertyPath
    * @return the field descriptor or null if not found
    * @throws IllegalStateException if the entity type is unknown
    */
   private FieldDescriptor getField(String entityType, List<String> propertyPath) {
      Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      int i = 0;
      for (String p : propertyPath) {
         FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null || ++i == propertyPath.size()) {
            return field;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            break;
         }
      }
      return null;
   }

   @Override
   public boolean hasProperty(String entityType, List<String> propertyPath) {
      Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      int i = 0;
      for (String p : propertyPath) {
         i++;
         FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            return false;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            break;
         }
      }
      return i == propertyPath.size();
   }

   @Override
   public boolean hasEmbeddedProperty(String entityType, List<String> propertyPath) {
      Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      for (String p : propertyPath) {
         FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            return false;
         }
         if (field.getJavaType() == JavaType.MESSAGE) {
            messageDescriptor = field.getMessageType();
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean isRepeatedProperty(String entityType, List<String> propertyPath) {
      Descriptor messageDescriptor;
      try {
         messageDescriptor = serializationContext.getMessageDescriptor(entityType);
      } catch (Exception e) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      for (String p : propertyPath) {
         FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null) {
            break;
         }
         if (field.isRepeated()) {
            return true;
         }
         if (field.getJavaType() != JavaType.MESSAGE) {
            break;
         }
         messageDescriptor = field.getMessageType();
      }
      return false;
   }

   @Override
   public Object convertToPropertyType(String entityType, List<String> propertyPath, String value) {
      FieldDescriptor field = getField(entityType, propertyPath);

      //todo [anistor] this is just for remote query because booleans and enums are handled as integers for historical reasons.
      if (field.getJavaType() == JavaType.BOOLEAN) {
         try {
            return Integer.parseInt(value) != 0;
         } catch (NumberFormatException e) {
            return super.convertToPropertyType(entityType, propertyPath, value);
         }
      } else if (field.getJavaType() == JavaType.ENUM) {
         EnumDescriptor enumType = field.getEnumType();
         EnumValueDescriptor enumValue;
         try {
            enumValue = enumType.findValueByNumber(Integer.parseInt(value));
         } catch (NumberFormatException e) {
            enumValue = enumType.findValueByName(value);
         }
         if (enumValue == null) {
            throw log.getInvalidEnumLiteralException(value, enumType.getFullName());
         }
         return enumValue.getNumber();
      }

      return super.convertToPropertyType(entityType, propertyPath, value);
   }
}
