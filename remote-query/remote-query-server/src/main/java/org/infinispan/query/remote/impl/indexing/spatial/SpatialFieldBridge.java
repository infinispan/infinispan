package org.infinispan.query.remote.impl.indexing.spatial;

import static java.util.Locale.ENGLISH;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.AppliedOnTypeAwareBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.bridge.MetadataProvidingFieldBridge;
import org.hibernate.search.bridge.spi.FieldMetadataBuilder;
import org.hibernate.search.bridge.spi.FieldType;
import org.hibernate.search.exception.AssertionFailure;
import org.hibernate.search.spatial.Coordinates;
import org.hibernate.search.spatial.impl.SpatialHelper;
import org.hibernate.search.util.logging.impl.Log;
import org.hibernate.search.util.logging.impl.LoggerFactory;

public abstract class SpatialFieldBridge implements MetadataProvidingFieldBridge, AppliedOnTypeAwareBridge {

   private static final Log LOG = LoggerFactory.make(MethodHandles.lookup());

   protected String latitudeField;
   protected String longitudeField;

   protected String latitudeIndexedFieldName;
   protected String longitudeIndexedFieldName;

   private MethodHandle latitudeHandle;
   private MethodHandle longitudeHandle;

   public SpatialFieldBridge() {
   }

   public SpatialFieldBridge(String latitudeField, String longitudeField) {
      this.latitudeField = latitudeField;
      this.longitudeField = longitudeField;
   }

   @Override
   public void setAppliedOnType(Class<?> returnType) {
      if (latitudeField != null && longitudeField != null) {
         latitudeHandle = getHandleFromName(returnType, latitudeField);
         longitudeHandle = getHandleFromName(returnType, longitudeField);
      }
   }

   @Override
   public abstract void set(String name, Object value, Document document, LuceneOptions luceneOptions);

   protected Double getLatitude(final Object value) {
      if (latitudeHandle != null) {
         return invokeHandle(latitudeHandle, value);
      } else {
         try {
            Coordinates coordinates = (Coordinates) value;
            return coordinates.getLatitude();
         } catch (ClassCastException e) {
            throw LOG.cannotExtractCoordinateFromObject(value.getClass().getName());
         }
      }
   }

   @Override
   public void configureFieldMetadata(String name, FieldMetadataBuilder builder) {
      latitudeIndexedFieldName = SpatialHelper.formatLatitude(name);
      longitudeIndexedFieldName = SpatialHelper.formatLongitude(name);

      builder.field(name, FieldType.DOUBLE)
            .sortable(true);
   }

   private MethodHandle getHandleFromName(Class<?> clazz, String coordinateField) {
      try {
         Field field = clazz.getField(coordinateField);
         return MethodHandles.lookup().unreflectGetter(field);
      } catch (NoSuchFieldException e) {
         try {
            PropertyDescriptor propertyDescriptor = new PropertyDescriptor(
                  coordinateField,
                  clazz,
                  "get" + capitalize(coordinateField),
                  null);
            Method getterMethod = propertyDescriptor.getReadMethod();
            if (getterMethod != null) {
               return MethodHandles.lookup().unreflect(getterMethod);
            } else {
               throw LOG.cannotReadFieldForClass(coordinateField, clazz.getName());
            }
         } catch (IllegalAccessException | IntrospectionException ex) {
            throw LOG.cannotReadFieldForClass(coordinateField, clazz.getName());
         }
      } catch (IllegalAccessException e) {
         throw LOG.cannotReadFieldForClass(coordinateField, clazz.getName());
      }
   }

   protected Double getLongitude(final Object value) {
      if (longitudeHandle != null) {
         return invokeHandle(longitudeHandle, value);
      } else {
         try {
            Coordinates coordinates = (Coordinates) value;
            return coordinates.getLongitude();
         } catch (ClassCastException e) {
            throw LOG.cannotExtractCoordinateFromObject(value.getClass().getName());
         }
      }
   }

   private Double invokeHandle(MethodHandle handle, Object value) {
      try {
         return (Double) handle.invoke(value);
      } catch (Throwable e) {
         if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
         } else if (e instanceof Error) {
            throw (Error) e;
         } else {
            throw new AssertionFailure("Getting a spatial value from " + handle + " failed", e);
         }
      }
   }

   public static String capitalize(final String name) {
      if (name == null || name.length() == 0) {
         return name;
      }
      return name.substring(0, 1).toUpperCase(ENGLISH) + name.substring(1);
   }
}
