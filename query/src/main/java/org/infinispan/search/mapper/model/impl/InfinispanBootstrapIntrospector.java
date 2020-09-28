package org.infinispan.search.mapper.model.impl;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.annotations.common.reflection.java.JavaReflectionManager;
import org.hibernate.search.mapper.pojo.model.hcann.spi.AbstractPojoHCAnnBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.hcann.spi.PojoHCannOrmGenericContextHelper;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoGenericTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.AssertionFailure;
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
   private final PojoHCannOrmGenericContextHelper genericContextHelper;
   private final RawTypeDeclaringContext<?> missingRawTypeDeclaringContext;

   private final Map<Class<?>, PojoRawTypeModel<?>> typeModelCache = new HashMap<>();

   public InfinispanBootstrapIntrospector(ValueReadHandleFactory valueReadHandleFactory) {
      super(new JavaReflectionManager());
      this.valueReadHandleFactory = valueReadHandleFactory;
      this.genericContextHelper = new PojoHCannOrmGenericContextHelper(this);
      this.missingRawTypeDeclaringContext = new RawTypeDeclaringContext<>(genericContextHelper, Object.class);
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> InfinispanRawTypeModel<T> typeModel(Class<T> clazz) {
      if (clazz.isPrimitive()) {
         /*
          * We'll never manipulate the primitive type, as we're using generics everywhere,
          * so let's consider every occurrence of the primitive type as an occurrence of its wrapper type.
          */
         clazz = (Class<T>) ReflectionHelper.getPrimitiveWrapperType(clazz);
      }
      return (InfinispanRawTypeModel<T>) typeModelCache.computeIfAbsent(clazz, this::createTypeModel);
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

   ValueReadHandle<?> createValueReadHandle(Member member) throws IllegalAccessException {
      if (member instanceof Method) {
         Method method = (Method) member;
         setAccessible(method);
         return valueReadHandleFactory.createForMethod(method);
      } else if (member instanceof Field) {
         Field field = (Field) member;
         setAccessible(field);
         return valueReadHandleFactory.createForField(field);
      } else {
         throw new AssertionFailure("Unexpected type for a " + Member.class.getName() + ": " + member);
      }
   }

   private <T> PojoRawTypeModel<T> createTypeModel(Class<T> clazz) {
      PojoRawTypeIdentifier<T> typeIdentifier = PojoRawTypeIdentifier.of(clazz);
      try {
         return new InfinispanRawTypeModel<>(this, typeIdentifier,
               new RawTypeDeclaringContext<>(genericContextHelper, clazz));
      } catch (RuntimeException e) {
         throw log.errorRetrievingTypeModel(clazz, e);
      }
   }

   private static void setAccessible(AccessibleObject member) {
      try {
         // always set accessible to true as it bypasses the security model checks
         // at execution time and is faster.
         member.setAccessible(true);
      } catch (SecurityException se) {
         if (!Modifier.isPublic(((Member) member).getModifiers())) {
            throw se;
         }
      }
   }
}
