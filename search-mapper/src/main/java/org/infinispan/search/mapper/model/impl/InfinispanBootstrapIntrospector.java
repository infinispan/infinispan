package org.infinispan.search.mapper.model.impl;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.hibernate.annotations.common.reflection.XClass;
import org.hibernate.annotations.common.reflection.java.JavaReflectionManager;
import org.hibernate.search.mapper.pojo.model.hcann.spi.AbstractPojoHCAnnBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoGenericTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.impl.ReflectionHelper;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandle;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandleFactory;
import org.infinispan.search.mapper.log.impl.Log;

/**
 * A very simple introspector roughly following Java Beans conventions.
 * <p>
 * As per JavaBeans conventions, only public getters are supported, and field access is not.
 */
public class InfinispanBootstrapIntrospector extends AbstractPojoHCAnnBootstrapIntrospector
      implements PojoBootstrapIntrospector {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

   private final ValueReadHandleFactory valueReadHandleFactory;
   private final InfinispanGenericContextHelper genericContextHelper;
   private final RawTypeDeclaringContext<?> missingRawTypeDeclaringContext;

   private final Map<Class<?>, PojoRawTypeModel<?>> typeModelCache = new HashMap<>();

   public InfinispanBootstrapIntrospector(ValueReadHandleFactory valueReadHandleFactory) {
      super(new JavaReflectionManager());
      this.valueReadHandleFactory = valueReadHandleFactory;
      this.genericContextHelper = new InfinispanGenericContextHelper(this);
      this.missingRawTypeDeclaringContext = new RawTypeDeclaringContext<>(genericContextHelper, Object.class);
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> InfinispanTypeModel<T> typeModel(Class<T> clazz) {
      if (clazz.isPrimitive()) {
         /*
          * We'll never manipulate the primitive type, as we're using generics everywhere,
          * so let's consider every occurrence of the primitive type as an occurrence of its wrapper type.
          */
         clazz = (Class<T>) ReflectionHelper.getPrimitiveWrapperType(clazz);
      }
      return (InfinispanTypeModel<T>) typeModelCache.computeIfAbsent(clazz, this::createTypeModel);
   }

   @Override
   public PojoRawTypeModel<?> typeModel(String name) {
      throw log.namedTypesNotSupported(name);
   }

   @Override
   public <T> PojoGenericTypeModel<T> genericTypeModel(Class<T> clazz) {
      return missingRawTypeDeclaringContext.createGenericTypeModel(clazz);
   }

   @Override
   public ValueReadHandleFactory annotationValueReadHandleFactory() {
      return valueReadHandleFactory;
   }

   Stream<? extends InfinispanTypeModel<?>> ascendingSuperTypes(XClass xClass) {
      return ascendingSuperClasses(xClass).map(this::typeModel);
   }

   Stream<? extends InfinispanTypeModel<?>> descendingSuperTypes(XClass xClass) {
      return descendingSuperClasses(xClass).map(this::typeModel);
   }

   ValueReadHandle<?> createValueReadHandle(Method method) throws IllegalAccessException {
      return valueReadHandleFactory.createForMethod(method);
   }

   private <T> PojoRawTypeModel<T> createTypeModel(Class<T> clazz) {
      PojoRawTypeIdentifier<T> typeIdentifier = PojoRawTypeIdentifier.of(clazz);
      try {
         return new InfinispanTypeModel<>(this, typeIdentifier,
               new RawTypeDeclaringContext<>(genericContextHelper, clazz));
      } catch (RuntimeException e) {
         throw log.errorRetrievingTypeModel(clazz, e);
      }
   }
}
