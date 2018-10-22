/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.functional;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import org.hibernate.Session;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.hibernate.cache.commons.util.EndInvalidationCommand;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.TombstoneUpdate;
import org.hibernate.cache.internal.SimpleCacheKeysFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;

import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.commons.util.ExpectingInterceptor;
import org.hibernate.testing.BeforeClassOnce;
import org.hibernate.testing.junit4.BaseNonConfigCoreFunctionalTestCase;
import org.hibernate.testing.junit4.CustomParameterized;
import org.infinispan.test.hibernate.cache.commons.tm.JtaPlatformImpl;
import org.infinispan.test.hibernate.cache.commons.tm.XaConnectionProvider;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.util.TxUtil;
import org.infinispan.AdvancedCache;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author Galder Zamarreño
 * @since 3.5
 */
@RunWith(CustomParameterized.class)
public abstract class AbstractFunctionalTest extends BaseNonConfigCoreFunctionalTestCase {
	protected static final Object[] TRANSACTIONAL = new Object[]{"transactional", JtaPlatformImpl.class, JtaTransactionCoordinatorBuilderImpl.class, XaConnectionProvider.class, AccessType.TRANSACTIONAL, CacheMode.INVALIDATION_SYNC, false };
	protected static final Object[] READ_WRITE_INVALIDATION = new Object[]{"read-write", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_WRITE, CacheMode.INVALIDATION_SYNC, false };
	protected static final Object[] READ_ONLY_INVALIDATION = new Object[]{"read-only", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_ONLY, CacheMode.INVALIDATION_SYNC, false };
	protected static final Object[] READ_WRITE_REPLICATED = new Object[]{"read-write", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_WRITE, CacheMode.REPL_SYNC, false };
	protected static final Object[] READ_ONLY_REPLICATED = new Object[]{"read-only", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_ONLY, CacheMode.REPL_SYNC, false };
	protected static final Object[] READ_WRITE_DISTRIBUTED = new Object[]{"read-write", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_WRITE, CacheMode.DIST_SYNC, false };
	protected static final Object[] READ_ONLY_DISTRIBUTED = new Object[]{"read-only", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_ONLY, CacheMode.DIST_SYNC, false };
	protected static final Object[] NONSTRICT_REPLICATED = new Object[]{"nonstrict", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.NONSTRICT_READ_WRITE, CacheMode.REPL_SYNC, true };
	protected static final Object[] NONSTRICT_DISTRIBUTED = new Object[]{"nonstrict", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.NONSTRICT_READ_WRITE, CacheMode.DIST_SYNC, true };

	// We need to use @ClassRule here since in @BeforeClassOnce startUp we're preparing the session factory,
	// constructing CacheManager along - and there we check that the test has the name already set
	@ClassRule
	public static final InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

	@Parameterized.Parameter(value = 0)
	public String mode;

	@Parameterized.Parameter(value = 1)
	public Class<? extends JtaPlatform> jtaPlatformClass;

	@Parameterized.Parameter(value = 2)
	public Class<?> transactionCoordinatorBuilderClass;

	@Parameterized.Parameter(value = 3)
	public Class<? extends ConnectionProvider> connectionProviderClass;

	@Parameterized.Parameter(value = 4)
	public AccessType accessType;

	@Parameterized.Parameter(value = 5)
	public CacheMode cacheMode;

	@Parameterized.Parameter(value = 6)
	public boolean addVersions;

	protected boolean useJta;
	protected List<Runnable> cleanup = new ArrayList<>();

	@CustomParameterized.Order(0)
	@Parameterized.Parameters(name = "{0}, {5}")
	public abstract List<Object[]> getParameters();

	public List<Object[]> getParameters(boolean tx, boolean rw, boolean ro, boolean nonstrict) {
		ArrayList<Object[]> parameters = new ArrayList<>();
		if (tx) {
			parameters.add(TRANSACTIONAL);
		}
		if (rw) {
			parameters.add(READ_WRITE_INVALIDATION);
			parameters.add(READ_WRITE_REPLICATED);
			parameters.add(READ_WRITE_DISTRIBUTED);
		}
		if (ro) {
			parameters.add(READ_ONLY_INVALIDATION);
			parameters.add(READ_ONLY_REPLICATED);
			parameters.add(READ_ONLY_DISTRIBUTED);
		}
		if (nonstrict) {
			parameters.add(NONSTRICT_REPLICATED);
			parameters.add(NONSTRICT_DISTRIBUTED);
		}
		return parameters;
	}

	@BeforeClassOnce
	public void setUseJta() {
		useJta = jtaPlatformClass != NoJtaPlatform.class;
	}

	@Override
	protected void prepareTest() throws Exception {
		infinispanTestIdentifier.joinContext();
	}

	@After
	public void runCleanup() {
		cleanup.forEach(Runnable::run);
		cleanup.clear();
	}

   @Override
   protected String getBaseForMappings() {
      return "org/infinispan/test/";
   }

   @Override
	public String[] getMappings() {
		return new String[] {
				"hibernate/cache/commons/functional/entities/Item.hbm.xml",
				"hibernate/cache/commons/functional/entities/Customer.hbm.xml",
				"hibernate/cache/commons/functional/entities/Contact.hbm.xml"
		};
	}

	@Override
	protected void afterMetadataBuilt(Metadata metadata) {
		if (addVersions) {
			for (PersistentClass clazz : metadata.getEntityBindings()) {
				if (clazz.getVersion() != null) {
					continue;
				}
				try {
					clazz.getMappedClass().getMethod("getVersion");
					clazz.getMappedClass().getMethod("setVersion", long.class);
				} catch (NoSuchMethodException e) {
					continue;
				}
				RootClass rootClazz = clazz.getRootClass();
				Property versionProperty = new Property();
				versionProperty.setName("version");
				SimpleValue value = new SimpleValue((MetadataImplementor) metadata, rootClazz.getTable());
				value.setTypeName("long");
				Column column = new Column();
				column.setValue(value);
				column.setName("version");
				value.addColumn(column);
				rootClazz.getTable().addColumn(column);
				versionProperty.setValue(value);
				rootClazz.setVersion(versionProperty);
				rootClazz.addProperty(versionProperty);
			}
		}
	}

	@Override
	public String getCacheConcurrencyStrategy() {
		return accessType.getExternalName();
	}

	protected boolean getUseQueryCache() {
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void addSettings(Map settings) {
		super.addSettings( settings );

		settings.put( Environment.USE_SECOND_LEVEL_CACHE, "true" );
		settings.put( Environment.GENERATE_STATISTICS, "true" );
		settings.put( Environment.USE_QUERY_CACHE, String.valueOf( getUseQueryCache() ) );
		settings.put( Environment.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass().getName() );
		settings.put( Environment.CACHE_KEYS_FACTORY, SimpleCacheKeysFactory.SHORT_NAME );
		settings.put( TestRegionFactory.TRANSACTIONAL, useTransactionalCache() );
		settings.put( TestRegionFactory.CACHE_MODE, cacheMode);

		settings.put( AvailableSettings.JTA_PLATFORM, jtaPlatformClass.getName() );
		settings.put( Environment.TRANSACTION_COORDINATOR_STRATEGY, transactionCoordinatorBuilderClass.getName() );
		if ( connectionProviderClass != null) {
			settings.put(Environment.CONNECTION_PROVIDER, connectionProviderClass.getName());
		}
	}

	protected void markRollbackOnly(Session session) {
		TxUtil.markRollbackOnly(useJta, session);
	}

	protected CountDownLatch expectAfterUpdate(AdvancedCache cache, int numUpdates) {
		return expectReadWriteKeyCommand(cache, FutureUpdate.class::isInstance, numUpdates);
	}

	protected CountDownLatch expectEvict(AdvancedCache cache, int numUpdates) {
		return expectReadWriteKeyCommand(cache, f -> f instanceof TombstoneUpdate && ((TombstoneUpdate) f).getValue() == null, numUpdates);
	}

	protected CountDownLatch expectReadWriteKeyCommand(AdvancedCache cache, Predicate<Object> valuePredicate, int numUpdates) {
		if (!cacheMode.isInvalidation()) {
			CountDownLatch latch = new CountDownLatch(numUpdates);
			ExpectingInterceptor.get(cache)
				.when((ctx, cmd) -> cmd instanceof ReadWriteKeyCommand && valuePredicate.test(((ReadWriteKeyCommand) cmd).getFunction()))
				.countDown(latch);
			cleanup.add(() -> ExpectingInterceptor.cleanup(cache));
			return latch;
		} else {
			return new CountDownLatch(0);
		}
	}

   protected CountDownLatch expectAfterEndInvalidation(AdvancedCache cache, int numInvalidates) {
      CountDownLatch latch = new CountDownLatch(numInvalidates);
      wrapInboundInvocationHandler(cache, handler -> new ExpectingInboundInvocationHandler(handler, latch));
      return latch;
   }

   protected void removeAfterEndInvalidationHandler(AdvancedCache cache) {
      wrapInboundInvocationHandler(cache, handler -> ((ExpectingInboundInvocationHandler) handler).getDelegate());
   }

	protected boolean useTransactionalCache() {
		return TestRegionFactoryProvider.load().supportTransactionalCaches() && accessType == AccessType.TRANSACTIONAL;
	}

	private static final class ExpectingInboundInvocationHandler extends AbstractDelegatingHandler {

      private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider
            .getLog(ExpectingInboundInvocationHandler.class);

      final CountDownLatch latch;

      public ExpectingInboundInvocationHandler(PerCacheInboundInvocationHandler delegate, CountDownLatch latch) {
         super(delegate);
         this.latch = latch;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof EndInvalidationCommand) {
            delegate.handle(command, response -> {
               latch.countDown();
               log.tracef("Latch after count down %s", latch);
               reply.reply(response);
            }, order);
         } else {
            delegate.handle(command, reply, order);
         }
      }

      PerCacheInboundInvocationHandler getDelegate() {
         return delegate;
      }
   }

}
