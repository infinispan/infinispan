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
package org.infinispan.loaders.jpa;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.GeneratedValue;
import javax.persistence.PersistenceException;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
   import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.LockSupportCacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.InfinispanCollections;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@CacheLoaderMetadata(configurationClass = JpaCacheStoreConfig.class)
public class JpaCacheStore extends LockSupportCacheStore<Integer> {
	private JpaCacheStoreConfig config;
	private AdvancedCache<?, ?> cache;
	private EntityManagerFactory emf;
	private EntityManagerFactoryRegistry emfRegistry;

	private final static byte BINARY_STREAM_DELIMITER = 100;

	@Override
	public void init(CacheLoaderConfig config, Cache<?, ?> cache,
			StreamingMarshaller m) throws CacheLoaderException {
		super.init(config, cache, m);
		this.cache = cache.getAdvancedCache();
		this.emfRegistry = this.cache.getComponentRegistry().getGlobalComponentRegistry().getComponent(EntityManagerFactoryRegistry.class);
		this.config = (JpaCacheStoreConfig) config;
	}
	
	@Override
	public void start() throws CacheLoaderException {
		super.start();
		
		try {
			this.emf = this.emfRegistry.getEntityManagerFactory(config.getPersistenceUnitName());
		} catch (PersistenceException e) {
			throw new JpaCacheLoaderException("Persistence Unit [" + this.config.getPersistenceUnitName() + "] not found", e);
		}
		
		ManagedType<?> mt;
		
		try {
			mt = emf.getMetamodel()
				.entity(this.config.getEntityClass());
		} catch (IllegalArgumentException e) {
			throw new JpaCacheLoaderException("Entity class [" + this.config.getEntityClassName() + " specified in configuration is not recognized by the EntityManagerFactory with Persistence Unit [" + this.config.getPersistenceUnitName() + "]", e);
		}
		
		if (!(mt instanceof IdentifiableType)) {
			throw new JpaCacheLoaderException(
					"Entity class must have one and only one identifier (@Id or @EmbeddedId)");
		}
		IdentifiableType<?> it = (IdentifiableType<?>) mt;
		if (!it.hasSingleIdAttribute()) {
			throw new JpaCacheLoaderException(
					"Entity class has more than one identifier.  It must have only one identifier.");
		}

		Type<?> idType = it.getIdType();
		Class<?> idJavaType = idType.getJavaType();

		if (idJavaType.isAnnotationPresent(GeneratedValue.class)) {
			throw new JpaCacheLoaderException(
					"Entity class has one identifier, but it must not have @GeneratedValue annotation");
		}

	}

	public EntityManagerFactory getEntityManagerFactory() {
		return emf;
	}

	@Override
	public void stop() throws CacheLoaderException {
		try {
		   this.emfRegistry.closeEntityManagerFactory(config.getPersistenceUnitName());
			super.stop();
		} catch (Throwable t) {
			throw new CacheLoaderException(
					"Exceptions occurred while stopping store", t);
		}
	}

	@Override
	public Class<? extends CacheLoaderConfig> getConfigurationClass() {
		return JpaCacheStoreConfig.class;
	}
	
	protected boolean isValidKeyType(Object key) {
		return emf.getMetamodel().entity(config.getEntityClass()).getIdType().getJavaType().isAssignableFrom(key.getClass());
	}

	@Override
	protected void clearLockSafe() throws CacheLoaderException {
		EntityManager em = emf.createEntityManager();
		EntityTransaction txn = em.getTransaction();

		try {
			txn.begin();

			String name = em.getMetamodel().entity(config.getEntityClass())
					.getName();
			Query query = em.createQuery("DELETE FROM " + name);
			query.executeUpdate();

			txn.commit();
		} catch (Exception e) {
			if (txn != null && txn.isActive())
				txn.rollback();
			throw new CacheLoaderException("Exception caught in clear()", e);
		} finally {
			em.close();
		}

	}
	
	@Override
	protected Set<InternalCacheEntry> loadAllLockSafe()
			throws CacheLoaderException {
		return loadLockSafe(-1);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Set<InternalCacheEntry> loadLockSafe(int maxEntries)
			throws CacheLoaderException {
	   
	   if (maxEntries == 0)
	      return InfinispanCollections.emptySet();

		EntityManager em = emf.createEntityManager();

		try {
			CriteriaBuilder cb = em.getCriteriaBuilder();
			CriteriaQuery cq = cb.createQuery(config.getEntityClass());
			cq.select(cq.from(config.getEntityClass()));

			TypedQuery q = em.createQuery(cq);
			if (maxEntries > 0)
			   q.setMaxResults(maxEntries);

			List list = q.getResultList();

			if (list == null || list.isEmpty()) {
				return Collections.emptySet();
			}

			PersistenceUnitUtil util = emf.getPersistenceUnitUtil();

			Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(
					list.size());
			for (Object o : list) {
				Object key = util.getIdentifier(o);
				result.add(new ImmortalCacheEntry(key, o));
			}

			return result;
		} finally {
			em.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Set<Object> loadAllKeysLockSafe(Set<Object> keysToExclude)
			throws CacheLoaderException {

		EntityManager em = emf.createEntityManager();

		try {
			CriteriaBuilder cb = em.getCriteriaBuilder();
			CriteriaQuery<Tuple> cq = cb.createTupleQuery();

			Root root = cq.from(config.getEntityClass());
			Type idType = root.getModel().getIdType();
			SingularAttribute idAttr = root.getModel().getId(
					idType.getJavaType());

			cq.multiselect(root.get(idAttr));
			List<Tuple> tuples = em.createQuery(cq).getResultList();

			Set<Object> keys = new HashSet<Object>();
			for (Tuple t : tuples) {
				Object id = t.get(0);
				if (includeKey(id, keysToExclude)) {
					keys.add(id);
				}
			}

			return keys;
		} finally {
			em.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void toStreamLockSafe(ObjectOutput oos)
			throws CacheLoaderException {

		EntityManager em = emf.createEntityManager();

		try {
			CriteriaBuilder cb = em.getCriteriaBuilder();
			CriteriaQuery cq = cb.createQuery(config.getEntityClass());
			cq.select(cq.from(config.getEntityClass()));

			TypedQuery q = em.createQuery(cq);

			Iterator it = q.getResultList().iterator();
			for (; it.hasNext();) {
				Object o = it.next();
				marshaller.objectToObjectStream(o, oos);
			}
			marshaller.objectToObjectStream(BINARY_STREAM_DELIMITER, oos);
		} catch (IOException e) {
			throw new CacheLoaderException("IO Exception in toStreamLockSafe",
					e);
		} finally {
			em.close();
		}
	}

	@Override
	protected void fromStreamLockSafe(ObjectInput ois)
			throws CacheLoaderException {

		long batchSize = 0;

		EntityManager em = emf.createEntityManager();
		EntityTransaction txn = em.getTransaction();
		try {
			Object o = marshaller.objectFromObjectStream(ois);

			txn.begin();
			while (o != null) {
				if (!o.getClass().isAnnotationPresent(Entity.class))
					break;

				em.merge(o);
				batchSize++;

				if (batchSize >= config.getBatchSize()) {
					em.flush();
					em.clear();
					batchSize = 0;
				}

				o = marshaller.objectFromObjectStream(ois);
			}

			txn.commit();
		} catch (InterruptedException e) {
			if (txn != null && txn.isActive())
				txn.rollback();

			Thread.currentThread().interrupt();
		} catch (Exception e) {
			if (txn != null && txn.isActive())
				txn.rollback();

			throw new CacheLoaderException(e);
		} finally {
			em.close();
		}
	}

	@Override
	protected boolean removeLockSafe(Object key, Integer lockingKey)
			throws CacheLoaderException {

		if (!isValidKeyType(key)) {
			return false;
		}
		
		EntityManager em = emf.createEntityManager();
		try {
			Object o = em.find(config.getEntityClass(), key);
			if (o == null) {
				return false;
			}

			EntityTransaction txn = em.getTransaction();
			try {
				txn.begin();
				em.remove(o);
				txn.commit();

				return true;
			} catch (Exception e) {
				if (txn != null && txn.isActive())
					txn.rollback();
				throw new CacheLoaderException(
						"Exception caught in removeLockSafe()", e);
			}
		} finally {
			em.close();
		}
	}

	@Override
	protected void storeLockSafe(InternalCacheEntry entry, Integer lockingKey)
			throws CacheLoaderException {
	   
		EntityManager em = emf.createEntityManager();

		Object o = entry.getValue();
		try {
			if (!config.getEntityClass().isAssignableFrom(o.getClass())) {
				throw new JpaCacheLoaderException(
						"This cache is configured with JPA CacheStore to only store values of type " + config.getEntityClassName());
			} else {
				EntityTransaction txn = em.getTransaction();
				Object id = emf.getPersistenceUnitUtil().getIdentifier(o);
				if (!entry.getKey().equals(id)) {
					throw new JpaCacheLoaderException(
							"Entity id value must equal to key of cache entry: "
									+ "key = [" + entry.getKey() + "], id = ["
									+ id + "]");
				}
				try {
					txn.begin();

					em.merge(o);

					txn.commit();
				} catch (Exception e) {
					if (txn != null && txn.isActive())
						txn.rollback();
					throw new CacheLoaderException(
							"Exception caught in store()", e);
				}
			}
		} finally {
			em.close();
		}

	}

	@Override
	protected InternalCacheEntry loadLockSafe(Object key, Integer lockingKey)
			throws CacheLoaderException {

		if (!isValidKeyType(key)) {
			return null;
		}
		
		EntityManager em = emf.createEntityManager();
		try {
			Object o = em.find(config.getEntityClass(), key);
			if (o == null)
				return null;

			return new ImmortalCacheEntry(key, o);
		} finally {
			em.close();
		}

	}

	@Override
	protected Integer getLockFromKey(Object key) throws CacheLoaderException {
		return key.hashCode() & 0xfffffc00;
	}

	protected boolean includeKey(Object key, Set<Object> keysToExclude) {
		return keysToExclude == null || !keysToExclude.contains(key);
	}

	@Override
	protected void purgeInternal() throws CacheLoaderException {
		// Immortal - no purging needed
	}

}
