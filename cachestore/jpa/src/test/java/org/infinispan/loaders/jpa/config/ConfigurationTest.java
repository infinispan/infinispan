/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 * 
 */
package org.infinispan.loaders.jpa.config;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.jpa.JpaCacheStore;
import org.infinispan.loaders.jpa.configuration.JpaCacheStoreConfiguration;
import org.infinispan.loaders.jpa.configuration.JpaCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jpa.entity.Document;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.loaders.jpa.entity.Vehicle;
import org.infinispan.loaders.jpa.entity.VehicleId;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
@Test(groups = "unit", testName = "loaders.jpa.configuration.ConfigurationTest")
public class ConfigurationTest {

	public void testConfigBuilder() {
		GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
				.globalJmxStatistics().transport().defaultTransport().build();

		Configuration cacheConfig = new ConfigurationBuilder().loaders()
				.addLoader(JpaCacheStoreConfigurationBuilder.class)
				.persistenceUnitName("org.infinispan.loaders.jpa.configurationTest")
				.entityClass(User.class).build();
		
		
		CacheLoaderConfiguration cacheLoaderConfig = cacheConfig.loaders().cacheLoaders().get(0);
		assert cacheLoaderConfig instanceof JpaCacheStoreConfiguration;
		JpaCacheStoreConfiguration jpaCacheLoaderConfig = (JpaCacheStoreConfiguration) cacheLoaderConfig;
		assert jpaCacheLoaderConfig.persistenceUnitName().equals("org.infinispan.loaders.jpa.configurationTest");
		assert jpaCacheLoaderConfig.entityClass().equals(User.class);

		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				globalConfig);

		cacheManager.defineConfiguration("userCache", cacheConfig);

		cacheManager.start();
		Cache<String, User> userCache = cacheManager.getCache("userCache");
		User user = new User();
		user.setUsername("rtsang");
		user.setFirstName("Ray");
		user.setLastName("Tsang");
		userCache.put(user.getUsername(), user);
		userCache.stop();
		cacheManager.stop();
	}

	public void testLegacyJavaConfig() {
		GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
				.globalJmxStatistics().transport().defaultTransport().build();

		Configuration cacheConfig = new ConfigurationBuilder()
				.loaders()
				.addStore()
				.cacheStore(new JpaCacheStore())
				.addProperty("persistenceUnitName",
						"org.infinispan.loaders.jpa.configurationTest")
				.addProperty("entityClassName",
						"org.infinispan.loaders.jpa.entity.User").build();

		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				globalConfig);

		cacheManager.defineConfiguration("userCache", cacheConfig);

		cacheManager.start();
		Cache<String, User> userCache = cacheManager.getCache("userCache");
		User user = new User();
		user.setUsername("rayt");
		user.setFirstName("Ray");
		user.setLastName("Tsang");
		userCache.put(user.getUsername(), user);
		userCache.stop();
		cacheManager.stop();
	}

	public void textXmlConfigLegacy() throws IOException {
		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				"config/jpa-config-legacy.xml");
		Cache<String, User> userCache = cacheManager.getCache("userCache");
		Cache<String, Document> docCache = cacheManager
            .getCache("documentCache");
      Cache<VehicleId, Vehicle> vehicleCache = cacheManager
            .getCache("vehicleCache");
      
		User user = new User();
		user.setUsername("jdoe");
		user.setFirstName("John");
		user.setLastName("Doe");
		userCache.put(user.getUsername(), user);
		
		Document doc = new Document();
		doc.setName("hello");
		doc.setTitle("Hello World");
		doc.setArticle("hello there... this is a test");
		docCache.put(doc.getName(), doc);
		
		Vehicle v = new Vehicle();
		v.setId(new VehicleId("CA", "123456"));
		v.setColor("RED");
		vehicleCache.put(v.getId(), v);
		
		userCache.stop();
		cacheManager.stop();
	}
	
	protected void validateConfig(Cache<VehicleId, Vehicle> vehicleCache) {
	   CacheLoaderConfiguration config = vehicleCache.getCacheConfiguration().loaders().cacheLoaders().get(0);
	   
	   if (config instanceof JpaCacheStoreConfiguration) {
	      JpaCacheStoreConfiguration jpaConfig = (JpaCacheStoreConfiguration) config;
	      assert jpaConfig.batchSize() == 1;
         assert jpaConfig.entityClass().equals(Vehicle.class) : jpaConfig.entityClass() + " != " + Vehicle.class;
         assert jpaConfig.persistenceUnitName().equals("org.infinispan.loaders.jpa.configurationTest") : jpaConfig.persistenceUnitName() + " != " + "org.infinispan.loaders.jpa.configurationTest";
	   } else {
	      assert false : "Unknown configuation class " + config.getClass();
	   }
	}
	
	public void testXmlConfig53() throws IOException {
		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				"config/jpa-config-53.xml");
		
		Cache<VehicleId, Vehicle> vehicleCache = cacheManager
				.getCache("vehicleCache");
		validateConfig(vehicleCache);
		
		Vehicle v = new Vehicle();
		v.setId(new VehicleId("NC", "123456"));
		v.setColor("BLUE");
		vehicleCache.put(v.getId(), v);

		vehicleCache.stop();
		cacheManager.stop();
	}
	
}
