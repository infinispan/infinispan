/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.hibernate.Session;
import org.hibernate.Version;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.internal.SimpleCacheKeysFactory;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.testing.TestForIssue;
import org.hibernate.testing.cache.CachingRegionFactory;
import org.hibernate.testing.junit4.BaseUnitTestCase;
import org.infinispan.test.hibernate.cache.commons.functional.entities.PK;
import org.infinispan.test.hibernate.cache.commons.functional.entities.WithEmbeddedId;
import org.infinispan.test.hibernate.cache.commons.functional.entities.WithSimpleId;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Gail Badner
 */
public class CacheKeySerializationTest extends BaseUnitTestCase {
	@Rule
	public InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

	private SessionFactoryImplementor getSessionFactory(String cacheKeysFactory) {
		Configuration configuration = new Configuration()
				.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true")
				.setProperty(Environment.CACHE_REGION_FACTORY, CachingRegionFactory.class.getName())
				.setProperty(Environment.DEFAULT_CACHE_CONCURRENCY_STRATEGY, "transactional")
				.setProperty("javax.persistence.sharedCache.mode", "ALL")
				.setProperty(Environment.HBM2DDL_AUTO, "create-drop");
		if (cacheKeysFactory != null) {
			configuration.setProperty(Environment.CACHE_KEYS_FACTORY, cacheKeysFactory);
		}
		configuration.addAnnotatedClass(WithSimpleId.class);
		configuration.addAnnotatedClass(WithEmbeddedId.class);
		return (SessionFactoryImplementor) configuration.buildSessionFactory();
	}

	@Test
	@TestForIssue(jiraKey = "HHH-11202")
	public void testSimpleCacheKeySimpleId() throws Exception {
		testId(SimpleCacheKeysFactory.INSTANCE, WithSimpleId.class.getName(), 1L);
	}

	@Test
	@TestForIssue(jiraKey = "HHH-11202")
	public void testSimpleCacheKeyEmbeddedId() throws Exception {
		testId(SimpleCacheKeysFactory.INSTANCE, WithEmbeddedId.class.getName(), new PK(1L));
	}

	@Test
	@TestForIssue(jiraKey = "HHH-11202")
	public void testDefaultCacheKeySimpleId() throws Exception {
		testId(DefaultCacheKeysFactory.INSTANCE, WithSimpleId.class.getName(), 1L);
	}

	@Test
	@TestForIssue(jiraKey = "HHH-11202")
	public void testDefaultCacheKeyEmbeddedId() throws Exception {
		testId(DefaultCacheKeysFactory.INSTANCE, WithEmbeddedId.class.getName(), new PK(1L));
	}

	private void testId(CacheKeysFactory cacheKeysFactory, String entityName, Object id) throws Exception {
		final SessionFactoryImplementor sessionFactory = getSessionFactory(cacheKeysFactory.getClass().getName());
		final EntityPersister persister = sessionFactory.getRuntimeMetamodels().getMappingMetamodel().getEntityDescriptor(entityName);
		final Object key = cacheKeysFactory.createEntityKey(
				id,
				persister,
				sessionFactory,
				null
		);

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject( key );

		final ObjectInputStream ois = new ObjectInputStream( new ByteArrayInputStream( baos.toByteArray() ) );
		final Object keyClone = ois.readObject();

		try {
			if (Version.getVersionString().contains("6.0")) {
				assertEquals( key, keyClone );
				assertEquals( keyClone, key );

				assertEquals( key.hashCode(), keyClone.hashCode() );

				final Object idClone = cacheKeysFactory.getEntityId( keyClone );

				assertEquals( id.hashCode(), idClone.hashCode() );
				assertEquals( id, idClone );
				assertEquals( idClone, id );
				assertTrue( persister.getIdentifierType().isEqual( id, idClone, sessionFactory ) );
				assertTrue( persister.getIdentifierType().isEqual( idClone, id, sessionFactory ) );
			} else {
				assertEquals(key, keyClone);
				assertEquals(keyClone, key);

				assertEquals(key.hashCode(), keyClone.hashCode());

				final Object idClone;
				if (cacheKeysFactory == SimpleCacheKeysFactory.INSTANCE) {
					idClone = cacheKeysFactory.getEntityId(keyClone);
				} else {
					// DefaultCacheKeysFactory#getEntityId will return a disassembled version
					try (Session session = sessionFactory.openSession()) {
						idClone = persister.getIdentifierType().assemble(
								(Serializable) cacheKeysFactory.getEntityId(keyClone),
								(SharedSessionContractImplementor) session,
								null
						);
					}
				}

				assertEquals(id.hashCode(), idClone.hashCode());
				assertEquals(id, idClone);
				assertEquals(idClone, id);
				assertTrue(persister.getIdentifierType().isEqual(id, idClone, sessionFactory));
				assertTrue(persister.getIdentifierType().isEqual(idClone, id, sessionFactory));
			}
		}
		finally {
			sessionFactory.close();
		}
	}
}
