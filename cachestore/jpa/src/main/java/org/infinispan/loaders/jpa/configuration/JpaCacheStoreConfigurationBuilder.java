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

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.jpa.JpaCacheStoreConfig;
import org.infinispan.util.TypedProperties;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
public class JpaCacheStoreConfigurationBuilder
		extends
		AbstractLockSupportStoreConfigurationBuilder<JpaCacheStoreConfiguration, JpaCacheStoreConfigurationBuilder> {
   
	private String persistenceUnitName;
	private Class<?> entityClass;
	private long batchSize = JpaCacheStoreConfig.DEFAULT_BATCH_SIZE;

	public JpaCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
		super(builder);
	}
	
	public JpaCacheStoreConfigurationBuilder persistenceUnitName(String persistenceUnitName) {
		this.persistenceUnitName = persistenceUnitName;
		return self();
	}
	
	public JpaCacheStoreConfigurationBuilder entityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
		return self();
	}
	
	public JpaCacheStoreConfigurationBuilder batchSize(long batchSize) {
	   this.batchSize = batchSize;
	   return self();
	}

	@Override
	public void validate() {
		// how do you validate required attributes?
		super.validate();
	}

	@Override
	public JpaCacheStoreConfiguration create() {
		return new JpaCacheStoreConfiguration(persistenceUnitName, entityClass, batchSize,
				lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications,
				TypedProperties.toTypedProperties(properties), async.create(),
				singletonStore.create());
	}

	@Override
	public Builder<?> read(JpaCacheStoreConfiguration template) {
		persistenceUnitName = template.persistenceUnitName();
		entityClass = template.entityClass();
		batchSize = template.batchSize();

		// LockSupportStore-specific configuration
		lockAcquistionTimeout = template.lockAcquistionTimeout();
		lockConcurrencyLevel = template.lockConcurrencyLevel();

		// AbstractStore-specific configuration
		fetchPersistentState = template.fetchPersistentState();
		ignoreModifications = template.ignoreModifications();
		properties = template.properties();
		purgeOnStartup = template.purgeOnStartup();
		purgeSynchronously = template.purgeSynchronously();
		this.async.read(template.async());
		this.singletonStore.read(template.singletonStore());
		
		return self();
	}

	@Override
	public JpaCacheStoreConfigurationBuilder self() {
		return this;
	}

}
