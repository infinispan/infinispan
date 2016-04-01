package org.infinispan.query.dsl.embedded.impl.jpalucene;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.hibernate.hql.ParsingException;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.TwoWayStringBridge;
import org.hibernate.search.bridge.builtin.NumericEncodingCalendarBridge;
import org.hibernate.search.bridge.builtin.NumericEncodingDateBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringEncodingCalendarBridge;
import org.hibernate.search.bridge.builtin.StringEncodingDateBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.builtin.impl.TwoWayString2FieldBridgeAdaptor;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
import org.hibernate.search.engine.metadata.impl.EmbeddedTypeMetadata;
import org.hibernate.search.engine.metadata.impl.PropertyMetadata;
import org.hibernate.search.engine.metadata.impl.TypeMetadata;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.query.logging.Log;
import org.jboss.logging.Logger;

import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class HibernateSearchPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   private static final Log log = Logger.getMessageLogger(Log.class, HibernateSearchPropertyHelper.class.getName());

   private final SearchIntegrator searchFactory;

   private final JPALuceneTransformer.FieldBridgeProvider fieldBridgeProvider;

   public HibernateSearchPropertyHelper(SearchIntegrator searchFactory, EntityNamesResolver entityNamesResolver, JPALuceneTransformer.FieldBridgeProvider fieldBridgeProvider) {
      super(entityNamesResolver);
      this.searchFactory = searchFactory;
      this.fieldBridgeProvider = fieldBridgeProvider;
   }

   @Override
   public Class<?> getEntityMetadata(String targetTypeName) {
      return entityNamesResolver.getClassFromName(targetTypeName);
   }

   /**
    * Returns the given value converted into the type of the given property as determined via the field bridge of the
    * property.
    *
    * @param value        the value to convert
    * @param entityType   the type hosting the property
    * @param propertyPath the name of the property
    * @return the given value converted into the type of the given property
    */
   @Override
   public Object convertToPropertyType(String entityType, List<String> propertyPath, String value) {
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      FieldBridge bridge = getFieldBridge(entityType, path);
      return convertToPropertyType(value, bridge);
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, String[] propertyPath) {
/* TODO [anistor] XMember is always null. There has to be another way to access hibernate search property metadata...
      XMember xm = getXMember(entityType, propertyPath);
      if (xm == null || isRepeatedXMember(xm)) {
         return null;
      }
      String name = xm.getType().getName();
      try {
         Class<?> propType = Class.forName(name);
         if (primitives.containsKey(propType)) {
            return primitives.get(propType);
         }
      } catch (ClassNotFoundException e) {
         // ignored
      }
*/
      return null;
   }

/*
   private XMember getXMember(String entityType, String[] propertyPath) {
      DocumentBuilderIndexedEntity docBuilder = getDocumentBuilder(entityType);
      if (isIdentifierProperty(docBuilder, propertyPath)) {
         return docBuilder.getIdGetter();
      }
      TypeMetadata typeMetadata = findTailTypeMetadata(docBuilder, propertyPath);
      if (typeMetadata == null) {
         return null;
      }
      String lastName = propertyPath[propertyPath.length - 1];
      PropertyMetadata propertyMetadata = typeMetadata.getPropertyMetadataForProperty(lastName);
      if (propertyMetadata == null) {
         return null;
      }
      return propertyMetadata.getPropertyAccessor();
   }

   private boolean isRepeatedXMember(XMember m) {
      return m.isArray() || m.isCollection();
   }
*/

   @Override
   public boolean isRepeatedProperty(String entityType, String[] propertyPath) {
      return false;
/*
      XMember xm = getXMember(entityType, propertyPath);
      return xm != null && isRepeatedXMember(xm);
*/
   }

   private Object convertToPropertyType(String value, FieldBridge bridge) {
      try {
         if (bridge instanceof NullEncodingTwoWayFieldBridge) {
            return convertToPropertyType(value, ((NullEncodingTwoWayFieldBridge) bridge).unwrap());
         } else if (bridge instanceof TwoWayString2FieldBridgeAdaptor) {
            return ((TwoWayString2FieldBridgeAdaptor) bridge).unwrap().stringToObject(value);
         } else if (bridge instanceof TwoWayStringBridge) {
            return ((TwoWayStringBridge) bridge).stringToObject(value);
         } else if (bridge instanceof NumericFieldBridge) {
            switch ((NumericFieldBridge) bridge) {
               case INT_FIELD_BRIDGE:
                  return Integer.valueOf(value);
               case LONG_FIELD_BRIDGE:
                  return Long.valueOf(value);
               case FLOAT_FIELD_BRIDGE:
                  return Float.valueOf(value);
               case DOUBLE_FIELD_BRIDGE:
                  return Double.valueOf(value);
               default:
                  return value;
            }
         } else if (bridge instanceof StringEncodingCalendarBridge || bridge instanceof NumericEncodingCalendarBridge) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DateTools.stringToDate(value));
            return calendar;
         } else if (bridge instanceof StringEncodingDateBridge || bridge instanceof NumericEncodingDateBridge) {
            return DateTools.stringToDate(value);
         } else {
            return value;
         }
      } catch (ParseException e) {
         throw new ParsingException(e);
      }
   }

   private FieldBridge getFieldBridge(String entityType, String[] propertyPath) {
      return fieldBridgeProvider != null ? fieldBridgeProvider.getFieldBridge(entityType, propertyPath)
            : getDefaultFieldBridge(entityType, propertyPath);
   }

   public FieldBridge getDefaultFieldBridge(String entityType, String[] propertyPath) {
      DocumentBuilderIndexedEntity docBuilder = getDocumentBuilder(entityType);
      if (isIdentifierProperty(docBuilder, propertyPath)) {
         return docBuilder.getIdBridge();
      }

      TypeMetadata typeMetadata = findTailTypeMetadata(docBuilder, propertyPath);
      if (typeMetadata == null) {
         return null;
      }
      String lastName = propertyPath[propertyPath.length - 1];
      PropertyMetadata propertyMetadata = typeMetadata.getPropertyMetadataForProperty(lastName);
      if (propertyMetadata == null) {
         return null;
      }

      // TODO for now we ignore properties composed of multiple fields
      return propertyMetadata.getFieldMetadataSet().iterator().next().getFieldBridge();
   }

   public boolean hasProperty(String typeName, String[] propertyPath) {
      DocumentBuilderIndexedEntity docBuilder = getDocumentBuilder(typeName);
      if (isIdentifierProperty(docBuilder, propertyPath)) {
         return true;
      }

      TypeMetadata typeMetadata = findTailTypeMetadata(docBuilder, propertyPath);
      if (typeMetadata == null) {
         return false;
      }
      String lastName = propertyPath[propertyPath.length - 1];
      return typeMetadata.getPropertyMetadataForProperty(lastName) != null
            || findEmbeddedTypeMetadata(typeMetadata, lastName) != null;
   }

   /**
    * Determines whether the given property path denotes an embedded entity (not a property of such entity).
    *
    * @param typeName     the indexed type
    * @param propertyPath the path of interest
    * @return {@code true} if the given path denotes an embedded entity of the given indexed type, {@code false}
    * otherwise.
    */
   public boolean hasEmbeddedProperty(String typeName, String[] propertyPath) {
      if (propertyPath.length == 0) {
         return false;
      }

      TypeMetadata typeMetadata = findTailTypeMetadata(typeName, propertyPath);
      String lastName = propertyPath[propertyPath.length - 1];
      return findEmbeddedTypeMetadata(typeMetadata, lastName) != null;
   }

   /**
    * Checks if the property of the indexed entity is analyzed.
    *
    * @param propertyPath the path of the property
    * @return {@code true} if the property is analyzed, {@code false} otherwise.
    */
   public boolean hasAnalyzedProperty(String typeName, String[] propertyPath) {
      DocumentBuilderIndexedEntity docBuilder = getDocumentBuilder(typeName);
      if (isIdentifierProperty(docBuilder, propertyPath)) {
         return false;
      }

      TypeMetadata typeMetadata = findTailTypeMetadata(docBuilder, propertyPath);
      if (typeMetadata == null) {
         return false;
      }
      String lastName = propertyPath[propertyPath.length - 1];
      DocumentFieldMetadata fieldMetadata = typeMetadata.getPropertyMetadataForProperty(lastName).getFieldMetadataSet().iterator().next();

      Index index = fieldMetadata.getIndex();
      return index == Field.Index.ANALYZED || index == Field.Index.ANALYZED_NO_NORMS;
   }

   private DocumentBuilderIndexedEntity getDocumentBuilder(String typeName) {
      Class<?> entityMetadata = getEntityMetadata(typeName);
      if (entityMetadata == null) {
         throw new IllegalStateException("Unknown entity name " + typeName);
      }

      EntityIndexBinding entityIndexBinding = searchFactory.getIndexBinding(entityMetadata);
      if (entityIndexBinding == null) {
         throw log.getNoIndexedEntityException(typeName);
      }
      return entityIndexBinding.getDocumentBuilder();
   }

   private boolean isIdentifierProperty(DocumentBuilderIndexedEntity documentBuilder, String[] propertyPath) {
      return propertyPath.length == 1 && propertyPath[0].equals(documentBuilder.getIdentifierName());
   }

   private TypeMetadata findTailTypeMetadata(DocumentBuilderIndexedEntity documentBuilder, String[] propertyPath) {
      TypeMetadata typeMetadata = documentBuilder.getMetadata();
      for (int i = 0; i < propertyPath.length - 1; i++) {
         typeMetadata = findEmbeddedTypeMetadata(typeMetadata, propertyPath[i]);
         if (typeMetadata == null) {
            break;
         }
      }
      return typeMetadata;
   }

   private TypeMetadata findTailTypeMetadata(String typeName, String[] propertyPath) {
      return findTailTypeMetadata(getDocumentBuilder(typeName), propertyPath);
   }

   private EmbeddedTypeMetadata findEmbeddedTypeMetadata(TypeMetadata typeMetadata, String name) {
      for (EmbeddedTypeMetadata embeddedTypeMetadata : typeMetadata.getEmbeddedTypeMetadata()) {
         if (embeddedTypeMetadata.getEmbeddedFieldName().equals(name)) {
            return embeddedTypeMetadata;
         }
      }
      return null;
   }
}
