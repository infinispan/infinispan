package org.infinispan.test.hibernate.cache.v62.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.Properties;

import org.hibernate.boot.internal.BootstrapContextImpl;
import org.hibernate.boot.internal.MetadataBuilderImpl;
import org.hibernate.boot.internal.SessionFactoryOptionsBuilder;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cache.cfg.internal.DomainDataRegionConfigImpl;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.mapping.Collection;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;

class TestRegionFactoryImpl implements TestRegionFactory {
   private static final Comparator<Object> UNIVERSAL_COMPARATOR = (o1, o2) -> {
      if (o1 instanceof Long && o2 instanceof Long) {
         return ((Long) o1).compareTo((Long) o2);
      } else if (o1 instanceof Integer && o2 instanceof Integer) {
         return ((Integer) o1).compareTo((Integer) o2);
      }
      throw new UnsupportedOperationException();
   };
   private final InfinispanRegionFactory delegate;

   TestRegionFactoryImpl(InfinispanRegionFactory delegate) {
      this.delegate = delegate;
   }

   @Override
   public void start(ServiceRegistry serviceRegistry, Properties p) {
      StandardServiceRegistry standardServiceRegistry = (StandardServiceRegistry) serviceRegistry;
      BootstrapContextImpl bootstrapContext = new BootstrapContextImpl(standardServiceRegistry, new MetadataBuilderImpl.MetadataBuildingOptionsImpl(standardServiceRegistry));
      SessionFactoryOptionsBuilder builder = new SessionFactoryOptionsBuilder(standardServiceRegistry, bootstrapContext);
      delegate.start(builder.buildOptions(), p);
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public void setCacheManager(EmbeddedCacheManager cm) {
      delegate.setCacheManager(cm);
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return delegate.getCacheManager();
   }

   @Override
   public String getBaseConfiguration(String regionName) {
      if (delegate instanceof TestInfinispanRegionFactory) {
         return ((TestInfinispanRegionFactory) delegate).getBaseConfiguration(regionName);
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public Configuration getConfigurationOverride(String regionName) {
      if (delegate instanceof TestInfinispanRegionFactory) {
         return ((TestInfinispanRegionFactory) delegate).getConfigurationOverride(regionName);
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public Configuration getPendingPutsCacheConfiguration() {
      return delegate.getPendingPutsCacheConfiguration();
   }

   @Override
   public InfinispanBaseRegion buildCollectionRegion(String regionName, AccessType accessType) {
      Collection collection = mock(Collection.class);
      when(collection.getRole()).thenReturn(regionName);
      when(collection.isMutable()).thenReturn(true);
      String rootClassName = regionName.indexOf('.') >= 0 ? regionName.substring(0, regionName.lastIndexOf('.')) : "";
      RootClass owner = rootClassMock(rootClassName);
      when(collection.getOwner()).thenReturn(owner);
      DomainDataRegionConfigImpl config = new DomainDataRegionConfigImpl.Builder(regionName).addCollectionConfig(collection, accessType).build();
      return (InfinispanBaseRegion) delegate.buildDomainDataRegion(config, null);
   }

   @Override
   public InfinispanBaseRegion buildEntityRegion(String regionName, AccessType accessType) {
      RootClass persistentClass = rootClassMock(regionName);
      DomainDataRegionConfigImpl config = new DomainDataRegionConfigImpl.Builder(regionName).addEntityConfig(persistentClass, accessType).build();
      return (InfinispanBaseRegion) delegate.buildDomainDataRegion(config, null);
   }

   private RootClass rootClassMock(String entityName) {
      RootClass persistentClass = mock(RootClass.class);
      when(persistentClass.getRootClass()).thenReturn(persistentClass);
      when(persistentClass.getEntityName()).thenReturn(entityName);
      Property versionMock = mock(Property.class);
      BasicType<Object> typeMock = mock(BasicType.class);
      JavaType<Object> javaType = mock(JavaType.class);
      when(javaType.getComparator()).thenReturn(UNIVERSAL_COMPARATOR);
      when(typeMock.getJavaTypeDescriptor()).thenReturn(javaType);
      when(versionMock.getType()).thenReturn(typeMock);
      when(persistentClass.getVersion()).thenReturn(versionMock);
      when(persistentClass.isVersioned()).thenReturn(true);
      when(persistentClass.isMutable()).thenReturn(true);

      return persistentClass;
   }

   @Override
   public InfinispanBaseRegion buildTimestampsRegion(String regionName) {
      return (InfinispanBaseRegion) delegate.buildTimestampsRegion(regionName, null);
   }

   @Override
   public InfinispanBaseRegion buildQueryResultsRegion(String regionName) {
      return (InfinispanBaseRegion) delegate.buildQueryResultsRegion(regionName, null);
   }

   @Override
   public RegionFactory unwrap() {
      return delegate;
   }

   @Override
   public long nextTimestamp() {
      return delegate.nextTimestamp();
   }
}
