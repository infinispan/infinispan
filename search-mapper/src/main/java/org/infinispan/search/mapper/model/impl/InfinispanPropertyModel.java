package org.infinispan.search.mapper.model.impl;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.stream.Stream;

import org.hibernate.annotations.common.reflection.XProperty;
import org.hibernate.search.mapper.pojo.model.hcann.spi.PojoCommonsAnnotationsHelper;
import org.hibernate.search.mapper.pojo.model.spi.PojoGenericTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandle;
import org.infinispan.search.mapper.log.impl.Log;

class InfinispanPropertyModel<T> implements PojoPropertyModel<T> {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

   private final InfinispanBootstrapIntrospector introspector;
   private final InfinispanTypeModel<?> parentTypeModel;

   private final XProperty property;
   private final Method readMethod;

   private PojoGenericTypeModel<T> typeModel;
   private ValueReadHandle<T> handle;

   InfinispanPropertyModel(InfinispanBootstrapIntrospector introspector, InfinispanTypeModel<?> parentTypeModel,
                           XProperty property) {
      this.introspector = introspector;
      this.parentTypeModel = parentTypeModel;
      this.property = property;
      this.readMethod = PojoCommonsAnnotationsHelper.extractUnderlyingMethod(property);
   }

   /**
    * N.B.: equals and hashCode must be defined properly
    * for {@link InfinispanGenericContextHelper#propertyCacheKey(PojoPropertyModel)}
    * to work properly.
    */
   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      InfinispanPropertyModel<?> that = (InfinispanPropertyModel<?>) o;
      return Objects.equals(introspector, that.introspector) && Objects.equals(parentTypeModel, that.parentTypeModel)
            && Objects.equals(handle(), that.handle());
   }

   @Override
   public int hashCode() {
      return Objects.hash(introspector, parentTypeModel, handle);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[" + name() + ", " + getGetterGenericReturnType().getTypeName() + "]";
   }

   @Override
   public String name() {
      return property.getName();
   }

   @Override
   public Stream<Annotation> annotations() {
      return introspector.annotations(property);
   }

   @Override
   /*
    * The cast is safe as long as both type parameter T and getGetterGenericReturnType
    * match the actual type for this property.
    */
   @SuppressWarnings("unchecked")
   public PojoGenericTypeModel<T> typeModel() {
      if (typeModel == null) {
         try {
            typeModel = (PojoGenericTypeModel<T>)
                  parentTypeModel.getRawTypeDeclaringContext().createGenericTypeModel(getGetterGenericReturnType());
         } catch (RuntimeException e) {
            throw log.errorRetrievingPropertyTypeModel(name(), parentTypeModel, e);
         }
      }
      return typeModel;
   }

   @Override
   @SuppressWarnings("unchecked") // By construction, we know the member returns values of type T
   public ValueReadHandle<T> handle() {
      if (handle == null) {
         try {
            handle = (ValueReadHandle<T>) introspector.createValueReadHandle(readMethod);
         } catch (IllegalAccessException | RuntimeException e) {
            throw log.errorRetrievingPropertyTypeModel(name(), parentTypeModel, e);
         }
      }
      return handle;
   }

   Type getGetterGenericReturnType() {
      return readMethod.getGenericReturnType();
   }
}
