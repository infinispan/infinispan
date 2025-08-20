package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.infinispan.query.objectfilter.impl.util.StringHelper;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.AnnotationElement;
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
public class ProtobufPropertyHelper extends ObjectPropertyHelper<Descriptor> {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, ProtobufPropertyHelper.class.getName());

   public static final String BIG_INTEGER_COMMON_TYPE = "org.infinispan.protostream.commons.BigInteger";
   public static final String BIG_DECIMAL_COMMON_TYPE = "org.infinispan.protostream.commons.BigDecimal";

   public static final String VERSION = VersionPropertyPath.VERSION_PROPERTY_NAME;
   public static final int VERSION_FIELD_ATTRIBUTE_ID = 150_000;
   public static final int MIN_METADATA_FIELD_ATTRIBUTE_ID = VERSION_FIELD_ATTRIBUTE_ID;

   public static final String VALUE = CacheValuePropertyPath.VALUE_PROPERTY_NAME;
   public static final int VALUE_FIELD_ATTRIBUTE_ID = 150_001;

   public static final String SCORE = ScorePropertyPath.SCORE_PROPERTY_NAME;
   public static final int SCORE_FIELD_ATTRIBUTE_ID = 150_002;

   public static final int KEY_FIELD_ATTRIBUTE_ID = 150_003;

   private static final IndexedFieldProvider.FieldIndexingMetadata<Descriptor> PROTO_NO_INDEXING = IndexedFieldProvider.noIndexing();
   private static final String EMBEDDED_ANNOTATION = "Embedded";
   private static final String STRUCTURE_ATTRIBUTE_NAME = "structure";
   private static final String NESTED_STRUCTURE_VALUE = "NESTED";

   private final EntityNameResolver<Descriptor> entityNameResolver;

   private final IndexedFieldProvider<Descriptor> indexedFieldProvider;

   public ProtobufPropertyHelper(SerializationContext serializationContext, IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      this(serializationContext::getMessageDescriptor, indexedFieldProvider);
   }

   public ProtobufPropertyHelper(EntityNameResolver<Descriptor> entityNameResolver, IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      this.entityNameResolver = entityNameResolver;
      this.indexedFieldProvider = indexedFieldProvider != null ? indexedFieldProvider :
            typeMetadata -> PROTO_NO_INDEXING;
   }

   @Override
   public IndexedFieldProvider<Descriptor> getIndexedFieldProvider() {
      return indexedFieldProvider;
   }

   @Override
   public Descriptor getEntityMetadata(String typeName) {
      return entityNameResolver.resolve(typeName);
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(Descriptor messageDescriptor, String[] propertyPath) {
      if (propertyPath.length == 1) {
         if (propertyPath[0].equals(VERSION)) {
            return Arrays.asList(VERSION_FIELD_ATTRIBUTE_ID);
         }
         if (propertyPath[0].equals(VALUE)) {
            return Arrays.asList(VALUE_FIELD_ATTRIBUTE_ID);
         }
         if (propertyPath[0].equals(SCORE)) {
            return Arrays.asList(SCORE_FIELD_ATTRIBUTE_ID);
         }
      }

      return translatePropertyPath(messageDescriptor, propertyPath);
   }

   @Override
   public Class<?> getPrimitivePropertyType(Descriptor entityType, String[] propertyPath) {
      if (propertyPath.length == 1) {
         if (propertyPath[0].equals(VERSION)) {
            return Long.class;
         }
         if (propertyPath[0].equals(VALUE)) {
            return Object.class;
         }
         if (propertyPath[0].equals(SCORE)) {
            return Float.class;
         }
      }

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
         case MESSAGE:
            switch (field.getTypeName()) {
               case BIG_INTEGER_COMMON_TYPE:
                  return BigInteger.class;
               case BIG_DECIMAL_COMMON_TYPE:
                  return BigDecimal.class;
            }
            return null;
      }
      return null;
   }

   @Override
   public Class<?> getIndexedPropertyType(Descriptor entityType, String[] propertyPath) {
      return getPrimitivePropertyType(entityType, propertyPath);
   }

   @Override
   public boolean isNestedIndexStructure(Descriptor entityType, String[] propertyPath) {
      FieldDescriptor field = getField(entityType, propertyPath);
      if (field == null) {
         return false;
      }
      Map<String, AnnotationElement.Annotation> annotations = field.getAnnotations();
      if (annotations == null) {
         return false;
      }
      AnnotationElement.Annotation annotation = annotations.get(EMBEDDED_ANNOTATION);
      if (annotation == null) {
         return false;
      }

      AnnotationElement.Value structure = annotation.getAttributeValue(STRUCTURE_ATTRIBUTE_NAME);
      if (structure == null) {
         return false;
      }
      return NESTED_STRUCTURE_VALUE.equals(structure.getValue());
   }

   /**
    * @param entityType   The owner of the field
    * @param propertyPath The path to search for
    * @return the field descriptor or null if not found
    */
   private FieldDescriptor getField(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;
      FieldDescriptor field = null;

      for (int i = 0; i < propertyPath.length; i++) {
         String property = propertyPath[i];
         field = messageDescriptor.findFieldByName(property);
         if (field != null) {
            if (field.getJavaType() == JavaType.MESSAGE) {
               // nesting for the next iteration
               messageDescriptor = field.getMessageType();
            }
         } else {
            if (i == 0) {
               messageDescriptor = indexedFieldProvider.get(messageDescriptor).keyType(property);
               if (messageDescriptor != null) {
                  field = syntheticKeyField(property, messageDescriptor.getFullName());
                  continue;
               }
            }
            // not found
            return null;
         }
      }
      return field;
   }

   private List<Integer> translatePropertyPath(Descriptor entityType, String[] propertyPath) {
      List<Integer> translatedPath = new ArrayList<>(propertyPath.length);
      Descriptor messageDescriptor = entityType;

      for (int i = 0; i < propertyPath.length; i++) {
         String property = propertyPath[i];
         FieldDescriptor field = messageDescriptor.findFieldByName(property);
         if (field != null) {
            translatedPath.add(field.getNumber());
            if (field.getJavaType() == JavaType.MESSAGE) {
               // nesting for the next iteration
               messageDescriptor = field.getMessageType();
            }
         } else {
            if (i == 0) {
               messageDescriptor = indexedFieldProvider.get(messageDescriptor).keyType(property);
               if (messageDescriptor != null) {
                  field = syntheticKeyField(property, messageDescriptor.getFullName());
                  translatedPath.add(field.getNumber());
                  continue;
               }
            }
            // not found
            return null;
         }
      }
      return translatedPath;
   }

   @Override
   public boolean isRepeatedProperty(Descriptor entityType, String[] propertyPath) {
      Descriptor messageDescriptor = entityType;

      for (int i = 0; i < propertyPath.length; i++) {
         String property = propertyPath[i];
         FieldDescriptor field = messageDescriptor.findFieldByName(property);
         if (field != null) {
            if (field.isRepeated()) {
               return true;
            }
            if (field.getJavaType() == JavaType.MESSAGE) {
               // nesting for the next iteration
               messageDescriptor = field.getMessageType();
            }
         } else {
            if (i == 0) {
               messageDescriptor = indexedFieldProvider.get(messageDescriptor).keyType(property);
               if (messageDescriptor != null) {
                  continue;
               }
            }
            // not found
            return false;
         }
      }
      return false;
   }

   @Override
   public boolean hasProperty(Descriptor entityType, String[] propertyPath) {
      if (propertyPath.length == 1 && (propertyPath[0].equals(VERSION) || propertyPath[0].equals(VALUE) ||
            propertyPath[0].equals(SCORE))) {
         return true;
      }
      return getField(entityType, propertyPath) != null;
   }

   @Override
   public boolean hasEmbeddedProperty(Descriptor entityType, String[] propertyPath) {
      FieldDescriptor field = getField(entityType, propertyPath);
      if (field == null) {
         return false;
      }
      return field.getJavaType() == JavaType.MESSAGE;
   }

   @Override
   public Object convertToPropertyType(Descriptor entityType, String[] propertyPath, String value) {
      FieldDescriptor field = getField(entityType, propertyPath);
      if (field == null) {
         return super.convertToPropertyType(entityType, propertyPath, value);
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

      if (field.getJavaType() == JavaType.MESSAGE) {
         if (field.getTypeName().equals(BIG_INTEGER_COMMON_TYPE)) {
            return new BigInteger(value);
         }
         if (field.getTypeName().equals(BIG_DECIMAL_COMMON_TYPE)) {
            return new BigDecimal(value);
         }
      }

      return super.convertToPropertyType(entityType, propertyPath, value);
   }

   public static FieldDescriptor syntheticKeyField(String name, String type) {
      return new FieldDescriptor.Builder()
            .withName(name)
            .withNumber(KEY_FIELD_ATTRIBUTE_ID)
            .withTypeName(type)
            .build();
   }
}
