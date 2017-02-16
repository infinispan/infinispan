package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.EnumDescriptor;
import org.infinispan.protostream.descriptors.EnumValueDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufPropertyHelper extends ObjectPropertyHelper<Descriptor> {

   private static final Log log = Logger.getMessageLogger(Log.class, ProtobufPropertyHelper.class.getName());

   private final SerializationContext serializationContext;

   private final IndexedFieldProvider<Descriptor> indexedFieldProvider;

   public ProtobufPropertyHelper(SerializationContext serializationContext, IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      this.serializationContext = serializationContext;
      this.indexedFieldProvider = indexedFieldProvider != null ? indexedFieldProvider : super.getIndexedFieldProvider();
   }

   @Override
   public IndexedFieldProvider<Descriptor> getIndexedFieldProvider() {
      return indexedFieldProvider;
   }

   @Override
   public Descriptor getEntityMetadata(String typeName) {
      return serializationContext.getMessageDescriptor(typeName);
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(Descriptor messageDescriptor, String[] propertyPath) {
      List<Integer> translatedPath = new ArrayList<>(propertyPath.length);
      Descriptor md = messageDescriptor;
      for (String prop : propertyPath) {
         FieldDescriptor fd = md.findFieldByName(prop);
         translatedPath.add(fd.getNumber());
         if (fd.getJavaType() == JavaType.MESSAGE) {
            md = fd.getMessageType();
         } else {
            md = null; // iteration is expected to stop here
         }
      }
      return translatedPath;
   }

   @Override
   public Class<?> getPrimitivePropertyType(Descriptor entityType, String[] propertyPath) {
      FieldDescriptor field = getField(entityType, propertyPath);
      if (field == null) {
         throw log.getNoSuchPropertyException(entityType.getFullName(), StringHelper.join(propertyPath));
      }
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
    */
   private FieldDescriptor getField(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;
      int i = 0;
      for (String p : propertyPath) {
         FieldDescriptor field = messageDescriptor.findFieldByName(p);
         if (field == null || ++i == propertyPath.length) {
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
   public boolean hasProperty(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;
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
      return i == propertyPath.length;
   }

   @Override
   public boolean hasEmbeddedProperty(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;
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
   public boolean isRepeatedProperty(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;
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
   public Object convertToPropertyType(Descriptor entityType, String[] propertyPath, String value) {
      FieldDescriptor field = getField(entityType, propertyPath);
      if (field == null) {
         throw log.getNoSuchPropertyException(entityType.getFullName(), StringHelper.join(propertyPath));
      }

      //todo [anistor] this is just for remote query because enums are handled as integers for historical reasons.
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
