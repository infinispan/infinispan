package org.infinispan.query.mapper.model.impl;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.engine.environment.classpath.spi.AggregatedClassLoader;
import org.hibernate.search.engine.environment.classpath.spi.ClassResolver;
import org.hibernate.search.engine.environment.classpath.spi.DefaultClassResolver;
import org.hibernate.search.engine.environment.classpath.spi.DefaultResourceResolver;
import org.hibernate.search.engine.environment.classpath.spi.ResourceResolver;
import org.hibernate.search.mapper.pojo.model.models.spi.AbstractPojoModelsBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.models.spi.PojoModelsGenericContextHelper;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.AssertionFailure;
import org.hibernate.search.util.common.impl.ReflectionHelper;
import org.hibernate.search.util.common.reflect.spi.ValueHandleFactory;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandle;
import org.infinispan.query.mapper.log.impl.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A very simple introspector roughly following Java Beans conventions.
 * <p>
 * As per JavaBeans conventions, only public getters are supported, and field access is not.
 */
public class InfinispanBootstrapIntrospector extends AbstractPojoModelsBootstrapIntrospector
      implements PojoBootstrapIntrospector {

   private static final Log log = LogFactory.getLog(InfinispanBootstrapIntrospector.class, Log.class);

   private final PojoModelsGenericContextHelper genericContextHelper;

   private final Map<Class<?>, PojoRawTypeModel<?>> typeModelCache = new HashMap<>();

   public InfinispanBootstrapIntrospector(ValueHandleFactory valueHandleFactory) {
      this(AggregatedClassLoader.createDefault(), valueHandleFactory);
   }

   public InfinispanBootstrapIntrospector(AggregatedClassLoader aggregatedClassLoader, ValueHandleFactory valueHandleFactory) {
      this(DefaultClassResolver.create(aggregatedClassLoader), DefaultResourceResolver.create(aggregatedClassLoader), valueHandleFactory);
   }

   public InfinispanBootstrapIntrospector(ClassResolver classResolver, ResourceResolver resourceResolver, ValueHandleFactory valueHandleFactory) {
      super(classResolver, resourceResolver, null, valueHandleFactory);
      this.genericContextHelper = new PojoModelsGenericContextHelper(this);
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

   protected ValueReadHandle<?> createValueReadHandle(Member member) throws IllegalAccessException {
      if (member instanceof Method) {
         Method method = (Method) member;
         setAccessible(method);
         return valueHandleFactory.createForMethod(method);
      } else if (member instanceof Field) {
         Field field = (Field) member;
         setAccessible(field);
         return valueHandleFactory.createForField(field);
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
