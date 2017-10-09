package org.infinispan.query.dsl.embedded.impl;

import java.beans.IntrospectionException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.hibernate.search.analyzer.impl.LuceneAnalyzerReference;
import org.hibernate.search.analyzer.spi.AnalyzerReference;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.TwoWayStringBridge;
import org.hibernate.search.bridge.builtin.BooleanBridge;
import org.hibernate.search.bridge.builtin.NumericEncodingCalendarBridge;
import org.hibernate.search.bridge.builtin.NumericEncodingDateBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringEncodingCalendarBridge;
import org.hibernate.search.bridge.builtin.StringEncodingDateBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.util.impl.TwoWayString2FieldBridgeAdaptor;
import org.hibernate.search.elasticsearch.bridge.builtin.impl.ElasticsearchDateBridge;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
import org.hibernate.search.engine.metadata.impl.EmbeddedTypeMetadata;
import org.hibernate.search.engine.metadata.impl.PropertyMetadata;
import org.hibernate.search.engine.metadata.impl.TypeMetadata;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.infinispan.query.logging.Log;
import org.jboss.logging.Logger;

/**
 * Uses the Hibernate Search metadata to resolve property paths. This relies on the Hibernate Search annotations. If
 * resolution fails (due to not annotated fields) the we delegate the process to the base class which works exclusively
 * with java-bean like reflection without relying on annotations.
 * <p>
 * Stateless and inherently threadsafe.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class HibernateSearchPropertyHelper extends ReflectionPropertyHelper {

   private static final Log log = Logger.getMessageLogger(Log.class, HibernateSearchPropertyHelper.class.getName());

   private final SearchIntegrator searchFactory;

   public HibernateSearchPropertyHelper(SearchIntegrator searchFactory, EntityNameResolver entityNameResolver) {
      super(entityNameResolver);
      this.searchFactory = searchFactory;
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(Class<?> entityType, String[] propertyPath) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(new PojoIndexedTypeIdentifier(entityType));
      if (indexBinding != null) {
         ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
         if (resolvedProperty != null) {
            List<String> translatedPropertyPath = new ArrayList<>(propertyPath.length);
            for (EmbeddedTypeMetadata embeddedTypeMetadata : resolvedProperty.embeddedTypeMetadataList) {
               translatedPropertyPath.add(embeddedTypeMetadata.getEmbeddedPropertyName());
            }
            if (resolvedProperty.propertyMetadata != null) {
               translatedPropertyPath.add(resolvedProperty.propertyMetadata.getPropertyAccessorName());
            }
            return translatedPropertyPath;
         }
      }
      return super.mapPropertyNamePathToFieldIdPath(entityType, propertyPath);
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
   public Object convertToPropertyType(Class<?> entityType, String[] propertyPath, String value) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding != null) {
         DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(indexBinding, propertyPath);
         if (fieldMetadata != null) {
            FieldBridge bridge = fieldMetadata.getFieldBridge();
            return convertToPropertyType(value, bridge);
         }
      }
      return super.convertToPropertyType(entityType, propertyPath, value);
   }

   private Object convertToPropertyType(String value, FieldBridge bridge) {
      try {
         if (bridge instanceof NullEncodingTwoWayFieldBridge) {
            return convertToPropertyType(value, ((NullEncodingTwoWayFieldBridge) bridge).unwrap(FieldBridge.class));
         } else if (bridge instanceof TwoWayString2FieldBridgeAdaptor) {
            TwoWayStringBridge unwrapped = ((TwoWayString2FieldBridgeAdaptor) bridge).unwrap(TwoWayStringBridge.class);
            if (unwrapped instanceof BooleanBridge) {
               if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                  throw log.getInvalidBooleanLiteralException(value);
               }
            }
            return unwrapped.stringToObject(value);
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
         } else if (bridge instanceof StringEncodingDateBridge || bridge instanceof NumericEncodingDateBridge || bridge instanceof ElasticsearchDateBridge) {
            return DateTools.stringToDate(value);
         } else {
            return value;
         }
      } catch (ParseException e) {
         throw new ParsingException(e);
      }
   }

   @Override
   public Class<?> getPrimitivePropertyType(Class<?> entityType, String[] propertyPath) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding != null) {
         ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
         if (resolvedProperty != null) {
            TypeMetadata typeMetadata;
            if (resolvedProperty.embeddedTypeMetadataList.isEmpty()) {
               typeMetadata = resolvedProperty.rootTypeMetadata;
            } else {
               typeMetadata = resolvedProperty.embeddedTypeMetadataList.get(resolvedProperty.embeddedTypeMetadataList.size() - 1);
            }
            if (resolvedProperty.propertyMetadata != null) {
               ReflectionHelper.PropertyAccessor accessor = getPropertyAccessor(typeMetadata.getType().getPojoType(), resolvedProperty.propertyMetadata.getPropertyAccessorName());
               Class<?> c = accessor.getPropertyType();
               if (c.isEnum()) {
                  return c;
               }
               return primitives.get(c);
            }
            return null;
         }
      }
      return super.getPrimitivePropertyType(entityType, propertyPath);
   }

   @Override
   public boolean isRepeatedProperty(Class<?> entityType, String[] propertyPath) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding != null) {
         ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
         if (resolvedProperty != null) {
            TypeMetadata typeMetadata = resolvedProperty.rootTypeMetadata;
            for (EmbeddedTypeMetadata embeddedTypeMetadata : resolvedProperty.embeddedTypeMetadataList) {
               ReflectionHelper.PropertyAccessor accessor = getPropertyAccessor(typeMetadata.getType().getPojoType(), embeddedTypeMetadata.getEmbeddedPropertyName());
               if (accessor.isMultiple()) {
                  return true;
               }
               typeMetadata = embeddedTypeMetadata;
            }
            if (resolvedProperty.propertyMetadata != null) {
               ReflectionHelper.PropertyAccessor accessor = getPropertyAccessor(typeMetadata.getType().getPojoType(), resolvedProperty.propertyMetadata.getPropertyAccessorName());
               return accessor.isMultiple();
            }
            return false;
         }
      }
      return super.isRepeatedProperty(entityType, propertyPath);
   }

   private EntityIndexBinding getEntityIndexBinding(Class<?> entityType) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding == null) {
         throw log.getNoIndexedEntityException(entityType.getCanonicalName());
      }
      return indexBinding;
   }

   private ReflectionHelper.PropertyAccessor getPropertyAccessor(Class<?> type, String propertyName) {
      try {
         return ReflectionHelper.getAccessor(type, propertyName);
      } catch (IntrospectionException e) {
         return null;
      }
   }

   public LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<Class<?>> getDefaultFieldBridgeProvider() {
      return new LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<Class<?>>() {

         @Override
         public FieldBridge getFieldBridge(Class<?> entityType, String[] propertyPath) {
            EntityIndexBinding indexBinding = getEntityIndexBinding(entityType);
            if (indexBinding != null) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(indexBinding, propertyPath);
               if (fieldMetadata != null) {
                  return fieldMetadata.getFieldBridge();
               }
            }
            return null;
         }

         @Override
         public Analyzer getAnalyzer(SearchIntegrator searchIntegrator, Class<?> entityType, String[] propertyPath) {
            EntityIndexBinding indexBinding = getEntityIndexBinding(entityType);
            if (indexBinding != null) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(indexBinding, propertyPath);
               if (fieldMetadata != null) {
                  AnalyzerReference analyzerReference = fieldMetadata.getAnalyzerReference();
                  if (analyzerReference.is(LuceneAnalyzerReference.class)) {
                     return analyzerReference.unwrap(LuceneAnalyzerReference.class).getAnalyzer();
                  }
               }
            }
            return null;
         }

         @Override
         public void overrideAnalyzers(IckleParsingResult<Class<?>> parsingResult, EntityContext entityContext) {
            // Hibernate Search populates the EntityContext; there's nothing more we need to do here.
         }
      };
   }

   @Override
   public boolean hasProperty(Class<?> entityType, String[] propertyPath) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding != null) {
         ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
         if (resolvedProperty != null) {
            return true;
         }
      }
      return super.hasProperty(entityType, propertyPath);
   }

   /**
    * Determines whether the given property path denotes an embedded entity (not a property of such entity).
    *
    * @param entityType   the indexed type
    * @param propertyPath the path of interest
    * @return {@code true} if the given path denotes an embedded entity of the given indexed type, {@code false}
    * otherwise.
    */
   @Override
   public boolean hasEmbeddedProperty(Class<?> entityType, String[] propertyPath) {
      EntityIndexBinding indexBinding = searchFactory.getIndexBindings().get(entityType);
      if (indexBinding != null) {
         ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
         if (resolvedProperty != null) {
            return resolvedProperty.propertyMetadata == null;
         }
      }
      return super.hasEmbeddedProperty(entityType, propertyPath);
   }

   private DocumentFieldMetadata getDocumentFieldMetadata(EntityIndexBinding indexBinding, String[] propertyPath) {
      ResolvedProperty resolvedProperty = resolveProperty(indexBinding, propertyPath);
      if (resolvedProperty != null && resolvedProperty.documentFieldMetadata != null) {
         return resolvedProperty.documentFieldMetadata;
      }
      return null;
   }

   @Override
   public IndexedFieldProvider<Class<?>> getIndexedFieldProvider() {
      return type -> {
         EntityIndexBinding entityIndexBinding = searchFactory.getIndexBindings().get(type);
         if (entityIndexBinding == null) {
            return IndexedFieldProvider.NO_INDEXING;
         }
         return new IndexedFieldProvider.FieldIndexingMetadata() {

            @Override
            public boolean isIndexed(String[] propertyPath) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(entityIndexBinding, propertyPath);
               return fieldMetadata != null && fieldMetadata.getIndex().isIndexed();
            }

            @Override
            public boolean isAnalyzed(String[] propertyPath) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(entityIndexBinding, propertyPath);
               return fieldMetadata != null && fieldMetadata.getIndex().isAnalyzed();
            }

            @Override
            public boolean isStored(String[] propertyPath) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(entityIndexBinding, propertyPath);
               return fieldMetadata != null && fieldMetadata.getStore() != Store.NO;
            }

            @Override
            public Object getNullMarker(String[] propertyPath) {
               DocumentFieldMetadata fieldMetadata = getDocumentFieldMetadata(entityIndexBinding, propertyPath);
               return fieldMetadata != null && fieldMetadata.getNullMarkerCodec() != null && fieldMetadata.getNullMarkerCodec().getNullMarker() != null
                     ? fieldMetadata.getNullMarkerCodec().getNullMarker().nullEncoded() : null;
            }
         };
      };
   }

   private static final class ResolvedProperty {

      final TypeMetadata rootTypeMetadata;

      final List<EmbeddedTypeMetadata> embeddedTypeMetadataList;

      final PropertyMetadata propertyMetadata;

      final DocumentFieldMetadata documentFieldMetadata;

      ResolvedProperty(TypeMetadata rootTypeMetadata, List<EmbeddedTypeMetadata> embeddedTypeMetadataList, DocumentFieldMetadata documentFieldMetadata, PropertyMetadata propertyMetadata) {
         this.rootTypeMetadata = rootTypeMetadata;
         this.embeddedTypeMetadataList = embeddedTypeMetadataList;
         this.documentFieldMetadata = documentFieldMetadata;
         this.propertyMetadata = propertyMetadata;
      }
   }

   private ResolvedProperty resolveProperty(EntityIndexBinding entityIndexBinding, String[] propertyPath) {
      if (propertyPath.length == 0) {
         return null;
      }
      DocumentBuilderIndexedEntity docBuilder = entityIndexBinding.getDocumentBuilder();
      TypeMetadata rootTypeMetadata = docBuilder.getTypeMetadata();
      TypeMetadata typeMetadata = rootTypeMetadata;
      List<EmbeddedTypeMetadata> embeddedTypeMetadataList = new ArrayList<>(propertyPath.length - 1);
      for (int i = 0; i < propertyPath.length; i++) {
         if (i == propertyPath.length - 1) {
            String propPath = StringHelper.join(propertyPath);
            DocumentFieldMetadata documentFieldMetadata = typeMetadata.getDocumentFieldMetadataFor(propPath);
            if (documentFieldMetadata != null) {
               for (PropertyMetadata pm : typeMetadata.getAllPropertyMetadata()) {
                  DocumentFieldMetadata fm = pm.getFieldMetadata(propPath);
                  if (fm != null) {
                     return new ResolvedProperty(rootTypeMetadata, embeddedTypeMetadataList, documentFieldMetadata, pm);
                  }
               }
               return null;
            }
         }

         boolean found = false;
         for (EmbeddedTypeMetadata embeddedTypeMetadata : typeMetadata.getEmbeddedTypeMetadata()) {
            if (embeddedTypeMetadata.getEmbeddedPropertyName().equals(propertyPath[i])) {
               embeddedTypeMetadataList.add(embeddedTypeMetadata);
               if (i == propertyPath.length - 1) {
                  return new ResolvedProperty(rootTypeMetadata, embeddedTypeMetadataList, null, null);
               }
               typeMetadata = embeddedTypeMetadata;
               found = true;
               break;
            }
         }
         if (!found) {
            break;
         }
      }
      return null;
   }
}
