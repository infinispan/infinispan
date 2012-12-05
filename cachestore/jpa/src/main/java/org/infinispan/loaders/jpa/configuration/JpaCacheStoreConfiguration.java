/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jpa.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jpa.JpaCacheStoreConfig;
import org.infinispan.util.TypedProperties;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@BuiltBy(JpaCacheStoreConfigurationBuilder.class)
public class JpaCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration implements LegacyLoaderAdapter<JpaCacheStoreConfig>{
	final private String persistenceUnitName;
	final private Class<?> entityClass;
	final private long batchSize;
	
	protected JpaCacheStoreConfiguration(
			String persistenceUnitName,
			Class<?> entityClass,
			long batchSize,
			long lockAcquistionTimeout,
			int lockConcurrencyLevel, boolean purgeOnStartup,
			boolean purgeSynchronously, int purgerThreads,
			boolean fetchPersistentState, boolean ignoreModifications,
			TypedProperties properties, AsyncStoreConfiguration async,
			SingletonStoreConfiguration singletonStore) {
		super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications, properties, async, singletonStore);
		
		this.persistenceUnitName = persistenceUnitName;
		this.entityClass = entityClass;
		this.batchSize = batchSize;
	}

	public String persistenceUnitName() {
		return persistenceUnitName;
	}
	
	public Class<?> entityClass() {
		return entityClass;
	}
	
	public long batchSize() {
	   return batchSize;
	}

	@Override
	public JpaCacheStoreConfig adapt() {
		JpaCacheStoreConfig config = new JpaCacheStoreConfig();
		
		LegacyConfigurationAdaptor.adapt(this, config);
		config.setPersistenceUnitName(persistenceUnitName);
		config.setEntityClass(entityClass);
		
		return config;
	}
	
	
}
