/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.util.CacheTestUtil;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;

/**
 * Defines the environment for a node.
 *
 * @author Steve Ebersole
 */
public class NodeEnvironment {
   private final StandardServiceRegistryBuilder ssrb;

   private StandardServiceRegistry serviceRegistry;
   private TestRegionFactory regionFactory;

   private Map<String, InfinispanBaseRegion> entityRegionMap;
   private Map<String, InfinispanBaseRegion> collectionRegionMap;

   public NodeEnvironment(StandardServiceRegistryBuilder ssrb) {
      this.ssrb = ssrb;
   }

   public StandardServiceRegistry getServiceRegistry() {
      return serviceRegistry;
   }

   public InfinispanBaseRegion getEntityRegion(String name, AccessType accessType) {
      if (entityRegionMap == null) {
         entityRegionMap = new HashMap<>();
         return buildAndStoreEntityRegion(name, accessType);
      }
      InfinispanBaseRegion region = entityRegionMap.get(name);
      if (region == null) {
         region = buildAndStoreEntityRegion(name, accessType);
      }
      return region;
   }

   private InfinispanBaseRegion buildAndStoreEntityRegion(String name, AccessType accessType) {
      InfinispanBaseRegion region = regionFactory.buildEntityRegion(name, accessType);
      entityRegionMap.put(name, region);
      return region;
   }

   public InfinispanBaseRegion getCollectionRegion(String name, AccessType accessType) {
      if (collectionRegionMap == null) {
         collectionRegionMap = new HashMap<>();
         return buildAndStoreCollectionRegion(name, accessType);
      }
      InfinispanBaseRegion region = collectionRegionMap.get(name);
      if (region == null) {
         region = buildAndStoreCollectionRegion(name, accessType);
         collectionRegionMap.put(name, region);
      }
      return region;
   }

   private InfinispanBaseRegion buildAndStoreCollectionRegion(String name, AccessType accessType) {
      return regionFactory.buildCollectionRegion(name, accessType);
   }

   public void prepare() throws Exception {
      serviceRegistry = ssrb.build();
      regionFactory = CacheTestUtil.startRegionFactory( serviceRegistry );
   }

   public void release() throws Exception {
      try {
         if (entityRegionMap != null) {
            for (InfinispanBaseRegion region : entityRegionMap.values()) {
               try {
                  region.getCache().stop();
               } catch (Exception e) {
                  // Ignore...
               }
            }
            entityRegionMap.clear();
         }
         if (collectionRegionMap != null) {
            for (InfinispanBaseRegion reg : collectionRegionMap.values()) {
               try {
                  reg.getCache().stop();
               } catch (Exception e) {
                  // Ignore...
               }
            }
            collectionRegionMap.clear();
         }
      }
      finally {
         try {
            if (regionFactory != null) {
               // Currently the RegionFactory is shutdown by its registration
               // with the CacheTestSetup from CacheTestUtil when built
               regionFactory.stop();
            }
         }
         finally {
            if (serviceRegistry != null) {
               StandardServiceRegistryBuilder.destroy( serviceRegistry );
            }
         }
      }
   }

   public RegionFactory getRegionFactory() {
      return regionFactory.unwrap();
   }
}
