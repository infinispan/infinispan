package org.infinispan.persistence.jpa;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityTransaction;
import javax.persistence.GeneratedValue;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Type;

import org.hibernate.Criteria;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.jpa.impl.EntityManagerFactoryRegistry;
import org.infinispan.persistence.jpa.impl.MetadataEntity;
import org.infinispan.persistence.jpa.impl.MetadataEntityKey;
import org.infinispan.persistence.jpa.impl.Stats;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@ConfiguredBy(JpaStoreConfiguration.class)
public class JpaStore implements AdvancedLoadWriteStore {
   private static final Log log = LogFactory.getLog(JpaStore.class);
   private static boolean trace = log.isTraceEnabled();

   private JpaStoreConfiguration configuration;
   private EntityManagerFactory emf;
   private EntityManagerFactoryRegistry emfRegistry;
   private StreamingMarshaller marshaller;
   private MarshalledEntryFactory marshallerEntryFactory;
   private TimeService timeService;
   private Stats stats = new Stats();
   private boolean setFetchSizeMinInteger = false;

   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.emfRegistry = ctx.getCache().getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry().getComponent(EntityManagerFactoryRegistry.class);
      this.marshallerEntryFactory = ctx.getMarshalledEntryFactory();
      this.marshaller = ctx.getMarshaller();
      this.timeService = ctx.getTimeService();
   }

   @Override
   public void start() {
      try {
         this.emf = emfRegistry.getEntityManagerFactory(configuration.persistenceUnitName());
      } catch (PersistenceException e) {
         throw new JpaStoreException("Persistence Unit [" + this.configuration.persistenceUnitName() + "] not found", e);
      }

      ManagedType<?> mt;
      try {
         mt = emf.getMetamodel().entity(this.configuration.entityClass());
      } catch (IllegalArgumentException e) {
         throw new JpaStoreException("Entity class [" + this.configuration.entityClass().getName() + " specified in configuration is not recognized by the EntityManagerFactory with Persistence Unit [" + this.configuration.persistenceUnitName() + "]", e);
      }

      if (!(mt instanceof IdentifiableType)) {
         throw new JpaStoreException(
               "Entity class must have one and only one identifier (@Id or @EmbeddedId)");
      }
      IdentifiableType<?> it = (IdentifiableType<?>) mt;
      if (!it.hasSingleIdAttribute()) {
         throw new JpaStoreException(
               "Entity class has more than one identifier.  It must have only one identifier.");
      }

      Type<?> idType = it.getIdType();
      Class<?> idJavaType = idType.getJavaType();

      if (idJavaType.isAnnotationPresent(GeneratedValue.class)) {
         throw new JpaStoreException(
               "Entity class has one identifier, but it must not have @GeneratedValue annotation");
      }

      // Hack: MySQL needs to have fetchSize set to Integer.MIN_VALUE in order to do streaming
      SessionFactory sessionFactory = emf.createEntityManager().unwrap(Session.class).getSessionFactory();
      if (sessionFactory instanceof SessionFactoryImplementor) {
         Dialect dialect = ((SessionFactoryImplementor) sessionFactory).getDialect();
         if (dialect instanceof MySQLDialect) {
            setFetchSizeMinInteger = true;
         }
      }
   }

   EntityManagerFactory getEntityManagerFactory() {
      return emf;
   }

   @Override
   public void stop() {
      try {
         emfRegistry.closeEntityManagerFactory(configuration.persistenceUnitName());
      } catch (Exception e) {
         throw new JpaStoreException("Exceptions occurred while stopping store", e);
      } finally {
         log.info("JPA Store stopped, stats: " + stats);
      }
   }

   protected boolean isValidKeyType(Object key) {
      return emf.getMetamodel().entity(configuration.entityClass()).getIdType().getJavaType().isAssignableFrom(key.getClass());
   }

   private Object findEntity(EntityManager em, Object key) {
      long begin = timeService.time();
      try {
         return em.find(configuration.entityClass(), key);
      } finally {
         stats.addEntityFind(timeService.time() - begin);
      }
   }

   private MetadataEntity findMetadata(EntityManager em, Object key) {
      long begin = timeService.time();
      try {
         return em.find(MetadataEntity.class, key);
      } finally {
         stats.addMetadataFind(timeService.time() - begin);
      }
   }

   private void removeEntity(EntityManager em, Object entity) {
      long begin = timeService.time();
      try {
         em.remove(entity);
      } finally {
         stats.addEntityRemove(timeService.time() - begin);
      }
   }

   private void removeMetadata(EntityManager em, MetadataEntity metadata) {
      long begin = timeService.time();
      try {
         em.remove(metadata);
      } finally {
         stats.addMetadataRemove(timeService.time() - begin);
      }
   }

   private void mergeEntity(EntityManager em, Object entity) {
      long begin = timeService.time();
      try {
         em.merge(entity);
      } finally {
         stats.addEntityMerge(timeService.time() - begin);
      }
   }

   private void mergeMetadata(EntityManager em, MetadataEntity metadata) {
      long begin = timeService.time();
      try {
         em.merge(metadata);
      } finally {
         stats.addMetadataMerge(timeService.time() - begin);
      }
   }

   @Override
   public void clear() {
      EntityManager emStream = emf.createEntityManager();
      try {
         ScrollableResults results = null;
         ArrayList<Object> batch = new ArrayList<Object>((int) configuration.batchSize());
         EntityTransaction txStream = emStream.getTransaction();
         txStream.begin();
         try {
            log.trace("Clearing JPA Store");
            Session session = emStream.unwrap(Session.class);
            Criteria criteria = session.createCriteria(configuration.entityClass()).setReadOnly(true)
                  .setProjection(Projections.id());
            if (setFetchSizeMinInteger) {
               criteria.setFetchSize(Integer.MIN_VALUE);
            }
            results = criteria.scroll(ScrollMode.FORWARD_ONLY);

            while (results.next()) {
               Object o = results.get(0);
               batch.add(o);
               if (batch.size() == configuration.batchSize()) {
                  session.clear();
                  removeBatch(batch);
               }
            }
            if (configuration.storeMetadata()) {
               /* We have to close the stream before executing further request */
               results.close();
               results = null;
               String metadataTable = emStream.getMetamodel().entity(MetadataEntity.class).getName();
               Query clearMetadata = emStream.createQuery("DELETE FROM " + metadataTable);
               clearMetadata.executeUpdate();
            }
            txStream.commit();
         } finally {
            removeBatch(batch);
            if (results != null) {
               results.close();
            }
            if (txStream != null && txStream.isActive()) {
               txStream.rollback();
            }
         }
      } catch (RuntimeException e) {
         log.error("Error in clear", e);
         throw e;
      } finally {
         emStream.close();
      }
   }

   private void removeBatch(ArrayList<Object> batch) {
      for (int i = 10; i >= 0; --i) {
         EntityManager emExec = emf.createEntityManager();
         EntityTransaction txn = null;
         try {
            txn = emExec.getTransaction();
            txn.begin();
            try {
               for (Object key : batch) {
                  try {
                     Object entity = emExec.getReference(configuration.entityClass(), key);
                     removeEntity(emExec, entity);
                  } catch (EntityNotFoundException e) {
                     log.trace("Cleared entity with key " + key + " not found", e);
                  }
               }
               txn.commit();
               batch.clear();
               break;
            } catch (Exception e) {
               if (i != 0) {
                  log.trace("Remove batch failed once", e);
               } else {
                  throw new JpaStoreException("Remove batch failing", e);
               }
            }
         } finally {
            if (txn != null && txn.isActive())
               txn.rollback();
            emExec.close();
         }
      }
   }

   public boolean delete(Object key) {
      if (!isValidKeyType(key)) {
         return false;
      }

      EntityManager em = emf.createEntityManager();
      try {
         Object entity = findEntity(em, key);
         if (entity == null) {
            return false;
         }
         MetadataEntity metadata = null;
         if (configuration.storeMetadata()) {
            byte[] keyBytes;
            try {
               keyBytes = marshaller.objectToByteBuffer(key);
            } catch (Exception e) {
               throw new JpaStoreException("Failed to marshall key", e);
            }
            metadata = findMetadata(em, new MetadataEntityKey(keyBytes));
         }

         EntityTransaction txn = em.getTransaction();
         if (trace) log.trace("Removing " + entity + "(" + toString(metadata) + ")");
         long txnBegin = timeService.time();
         txn.begin();
         try {
            removeEntity(em, entity);
            if (metadata != null) {
               removeMetadata(em, metadata);
            }
            txn.commit();
            stats.addRemoveTxCommitted(timeService.time() - txnBegin);
            return true;
         } catch (Exception e) {
            stats.addRemoveTxFailed(timeService.time() - txnBegin);
            throw new JpaStoreException("Exception caught in delete()", e);
         } finally {
            if (txn != null && txn.isActive())
               txn.rollback();
         }
      } finally {
         em.close();
      }
   }

   @Override
   public void write(MarshalledEntry entry) {
      EntityManager em = emf.createEntityManager();

      Object entity = entry.getValue();
      MetadataEntity metadata = configuration.storeMetadata() ?
            new MetadataEntity(entry.getKeyBytes(), entry.getMetadataBytes(),
                  entry.getMetadata() == null ? Long.MAX_VALUE : entry.getMetadata().expiryTime()) : null;
      try {
         if (!configuration.entityClass().isAssignableFrom(entity.getClass())) {
            throw new JpaStoreException(String.format(
                  "This cache is configured with JPA CacheStore to only store values of type %s - cannot write %s = %s",
                  configuration.entityClass().getName(), entity, entity.getClass().getName()));
         } else {
            EntityTransaction txn = em.getTransaction();
            Object id = emf.getPersistenceUnitUtil().getIdentifier(entity);
            if (!entry.getKey().equals(id)) {
               throw new JpaStoreException(
                     "Entity id value must equal to key of cache entry: "
                           + "key = [" + entry.getKey() + "], id = ["
                           + id + "]");
            }
            long txnBegin = timeService.time();
            try {
               if (trace) log.trace("Writing " + entity + "(" + toString(metadata) + ")");
               txn.begin();

               mergeEntity(em, entity);
               if (metadata != null && metadata.hasBytes()) {
                  mergeMetadata(em, metadata);
               }

               txn.commit();
               stats.addWriteTxCommited(timeService.time() - txnBegin);
            } catch (Exception e) {
               stats.addWriteTxFailed(timeService.time() - txnBegin);
               throw new JpaStoreException("Exception caught in write()", e);
            } finally {
               if (txn != null && txn.isActive())
                  txn.rollback();
            }
         }
      } finally {
         em.close();
      }

   }

   @Override
   public boolean contains(Object key) {
      if (!isValidKeyType(key)) {
         return false;
      }

      EntityManager em = emf.createEntityManager();
      try {
         EntityTransaction txn = em.getTransaction();
         long txnBegin = timeService.time();
         txn.begin();
         try {
            Object entity = findEntity(em, key);
            if (trace) log.trace("Entity " + key + " -> " + entity);
            try {
               if (entity == null) return false;
               if (configuration.storeMetadata()) {
                  byte[] keyBytes;
                  try {
                     keyBytes = marshaller.objectToByteBuffer(key);
                  } catch (Exception e) {
                     throw new JpaStoreException("Cannot marshall key", e);
                  }
                  MetadataEntity metadata = findMetadata(em, new MetadataEntityKey(keyBytes));
                  if (trace) log.trace("Metadata " + key + " -> " + toString(metadata));
                  return metadata == null || metadata.getExpiration() > timeService.wallClockTime();
               } else {
                  return true;
               }
            } finally {
               txn.commit();
               stats.addReadTxCommitted(timeService.time() - txnBegin);
            }
         } catch (RuntimeException e) {
            stats.addReadTxFailed(timeService.time() - txnBegin);
            throw e;
         } finally {
            if (txn != null && txn.isActive())
               txn.rollback();
         }
      } finally {
         em.close();
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      if (!isValidKeyType(key)) {
         return null;
      }

      EntityManager em = emf.createEntityManager();
      try {
         EntityTransaction txn = em.getTransaction();
         long txnBegin = timeService.time();
         txn.begin();
         try {
            Object entity = findEntity(em, key);
            try {
               if (entity == null)
                  return null;
               InternalMetadata m = null;
               if (configuration.storeMetadata()) {
                  byte[] keyBytes;
                  try {
                     keyBytes = marshaller.objectToByteBuffer(key);
                  } catch (Exception e) {
                     throw new JpaStoreException("Failed to marshall key", e);
                  }
                  MetadataEntity metadata = findMetadata(em, new MetadataEntityKey(keyBytes));
                  if (metadata != null && metadata.getMetadata() != null) {
                     try {
                        m = (InternalMetadata) marshaller.objectFromByteBuffer(metadata.getMetadata());
                     } catch (Exception e) {
                        throw new JpaStoreException("Failed to unmarshall metadata", e);
                     }
                     if (m.isExpired(timeService.wallClockTime())) {
                        return null;
                     }
                  }
               }
               if (trace) log.trace("Loaded " + entity + " (" + m + ")");
               return marshallerEntryFactory.newMarshalledEntry(key, entity, m);
            } finally {
               try {
                  txn.commit();
                  stats.addReadTxCommitted(timeService.time() - txnBegin);
               } catch (Exception e) {
                  stats.addReadTxFailed(timeService.time() - txnBegin);
                  throw new JpaStoreException("Failed to load entry", e);
               }
            }
         } finally {
            if (txn != null && txn.isActive())
               txn.rollback();
         }
      } finally {
         em.close();
      }
   }

   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      if (fetchMetadata && !configuration.storeMetadata()) {
         log.debug("Metadata cannot be retrieved as JPA Store is not configured to persist metadata.");
         fetchMetadata = false;
      }
      // We cannot stream entities as a full table as the entity can contain collections in another tables.
      // Then, Hibernate uses left outer joins which give us several rows of results for each entity.
      // We cannot use DISTINCT_ROOT_ENTITY transformer when streaming, that's only available for list()
      // and this is prone to OOMEs. Theoretically, custom join is possible but it is out of scope
      // of current JpaStore implementation.
      // We also can't switch JOINs to SELECTs as some DBs (e.g. MySQL) fails when another command is executed
      // on the connection when streaming.
      // Therefore, we can only query IDs and do a findEntity for each fetch.

      // Another problem: even for fetchValue=false and fetchMetadata=true, we cannot iterate over metadata
      // table, because this table does not have records for keys without metadata. With such iteration,
      // we wouldn't iterate over all keys - therefore, we can iterate only over entity table IDs and we
      // have to request the metadata in separate connection.
      if (fetchValue || fetchMetadata) {
         final boolean fv = fetchValue;
         final boolean fm = fetchMetadata;
         process(filter, task, executor, new ProcessStrategy() {
            @Override
            public Criteria getCriteria(Session session) {
               return session.createCriteria(configuration.entityClass()).setProjection(Projections.id());
            }

            @Override
            public Object getKey(Object scrollResult) {
               return scrollResult;
            }

            @Override
            public Callable<Void> getTask(CacheLoaderTask task, TaskContext taskContext, Object scrollResult, Object key) {
               return new LoadingProcessTask(task, taskContext, key, fv, fm);
            }
         });
      } else {
         process(filter, task, executor, new ProcessStrategy() {
            @Override
            public Criteria getCriteria(Session session) {
               return session.createCriteria(configuration.entityClass()).setProjection(Projections.id());
            }

            @Override
            public Object getKey(Object scrollResult) {
               return scrollResult;
            }

            @Override
            public Callable<Void> getTask(CacheLoaderTask task, TaskContext taskContext, Object scrollResult, Object key) {
               return new ProcessTask(task, taskContext, key, null, null);
            }
         });
      }
   }

   private void process(KeyFilter filter, final CacheLoaderTask task, Executor executor, ProcessStrategy strategy) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      TaskContextImpl taskContext = new TaskContextImpl();
      EntityManager emStream = emf.createEntityManager();
      try {
         EntityTransaction txStream = emStream.getTransaction();
         ScrollableResults results = null;
         txStream.begin();
         try {
            Session session = emStream.unwrap(Session.class);
            Criteria criteria = strategy.getCriteria(session).setReadOnly(true);
            if (setFetchSizeMinInteger) {
               criteria.setFetchSize(Integer.MIN_VALUE);
            }
            results = criteria.scroll(ScrollMode.FORWARD_ONLY);
            try {
               while (results.next()) {
                  if (taskContext.isStopped())
                     break;
                  Object result = results.get(0);
                  Object key = strategy.getKey(result);
                  if (filter != null && !filter.accept(key)) {
                     if (trace) log.trace("Key " + key + " filtered");
                     continue;
                  }
                  eacs.submit(strategy.getTask(task, taskContext, result, key));
               }
            } finally {
               if (results != null) results.close();
            }
            txStream.commit();
         } finally {
            if (txStream != null && txStream.isActive()) {
               txStream.rollback();
            }
         }
      } finally {
         emStream.close();
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new org.infinispan.persistence.spi.PersistenceException("Execution exception!", eacs.getFirstException());
      }
   }

   private InternalMetadata getMetadata(EntityManager em, Object key) {
      byte[] keyBytes;
      try {
         keyBytes = marshaller.objectToByteBuffer(key);
      } catch (Exception e) {
         throw new JpaStoreException("Failed to marshall key", e);
      }
      MetadataEntity m = findMetadata(em, new MetadataEntityKey(keyBytes));
      if (m == null) return null;

      try {
         return (InternalMetadata) marshaller.objectFromByteBuffer(m.getMetadata());
      } catch (Exception e) {
         throw new JpaStoreException("Failed to unmarshall metadata", e);
      }
   }

   @Override
   public int size() {
      EntityManager em = emf.createEntityManager();
      EntityTransaction txn = em.getTransaction();
      txn.begin();
      try {
         CriteriaBuilder builder = em.getCriteriaBuilder();
         CriteriaQuery<Long> cq = builder.createQuery(Long.class);
         cq.select(builder.count(cq.from(configuration.entityClass())));
         return em.createQuery(cq).getSingleResult().intValue();
      } finally {
         try {
            txn.commit();
         } finally {
            if (txn != null && txn.isActive()) {
               txn.rollback();
            }
         }
         em.close();
      }
   }

   @Override
   public void purge(Executor threadPool, final PurgeListener listener) {
      if (!configuration.storeMetadata()) {
         log.debug("JPA Store cannot be purged as metadata holding expirations are not available");
         return;
      }
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(threadPool);
      EntityManager emStream = emf.createEntityManager();
      try {
         EntityTransaction txStream = emStream.getTransaction();
         ScrollableResults metadataKeys = null;
         txStream.begin();
         try {
            long currentTime = timeService.wallClockTime();
            Session session = emStream.unwrap(Session.class);
            Criteria criteria = session.createCriteria(MetadataEntity.class).setReadOnly(true)
                  .add(Restrictions.le(MetadataEntity.EXPIRATION, currentTime)).setProjection(Projections.id());
            if (setFetchSizeMinInteger) {
               criteria.setFetchSize(Integer.MIN_VALUE);
            }

            metadataKeys = criteria.scroll(ScrollMode.FORWARD_ONLY);
            ArrayList<MetadataEntityKey> batch = new ArrayList<MetadataEntityKey>((int) configuration.batchSize());
            while (metadataKeys.next()) {
               MetadataEntityKey mKey = (MetadataEntityKey) metadataKeys.get(0);
               batch.add(mKey);
               if (batch.size() == configuration.batchSize()) {
                  purgeBatch(batch, listener, eacs, currentTime);
                  batch.clear();
               }
            }
            purgeBatch(batch, listener, eacs, currentTime);
            txStream.commit();
         } finally {
            if (metadataKeys != null) metadataKeys.close();
            if (txStream != null && txStream.isActive()) {
               txStream.rollback();
            }
         }
      } finally {
         emStream.close();
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new JpaStoreException(eacs.getFirstException());
      }
   }

   private void purgeBatch(List<MetadataEntityKey> batch, final PurgeListener listener, ExecutorAllCompletionService eacs, long currentTime) {
      EntityManager emExec = emf.createEntityManager();
      try {
         EntityTransaction txn = emExec.getTransaction();
         txn.begin();
         try {
            for (MetadataEntityKey metadataKey : batch) {
               MetadataEntity metadata = findMetadata(emExec, metadataKey);
               // check for transaction - I hope write skew check is done here
               if (metadata.getExpiration() > currentTime) {
                  continue;
               }

               final Object key;
               try {
                  key = marshaller.objectFromByteBuffer(metadata.getKeyBytes());
               } catch (Exception e) {
                  throw new JpaStoreException("Cannot unmarshall key", e);
               }
               Object entity = null;
               try {
                  entity = emExec.getReference(configuration.entityClass(), key);
                  removeEntity(emExec, entity);
               } catch (EntityNotFoundException e) {
                  log.trace("Expired entity with key " + key + " not found", e);
               }
               removeMetadata(emExec, metadata);

               if (trace) log.trace("Expired " + key + " -> " + entity + "(" + toString(metadata) + ")");
               if (listener != null) {
                  eacs.submit(new Runnable() {
                     @Override
                     public void run() {
                        listener.entryPurged(key);
                     }
                  }, null);
               }
            }
            txn.commit();
         } finally {
            if (txn != null && txn.isActive()) {
               txn.rollback();
            }
         }
      } finally {
         emExec.close();
      }
   }

   private String toString(MetadataEntity metadata) {
      if (metadata == null || !metadata.hasBytes()) return "<no metadata>";
      try {
         return marshaller.objectFromByteBuffer(metadata.getMetadata()).toString();
      } catch (Exception e) {
         log.trace("Failed to unmarshall metadata", e);
         return "<metadata: " + e + ">";
      }
   }

   private interface ProcessStrategy {
      Criteria getCriteria(Session session);
      Object getKey(Object scrollResult);
      Callable<Void> getTask(CacheLoaderTask task, TaskContext taskContext, Object scrollResult, Object key);
   }

   private class ProcessTask implements Callable<Void> {
      private final CacheLoaderTask task;
      private final TaskContext taskContext;
      private final Object key;
      private final Object entity;
      private final InternalMetadata metadata;

      private ProcessTask(CacheLoaderTask task, TaskContext taskContext, Object key, Object entity, InternalMetadata metadata) {
         this.task = task;
         this.taskContext = taskContext;
         this.key = key;
         this.entity = entity;
         this.metadata = metadata;
         if (trace) {
            log.tracef("Created process task with key=%s, value=%s, metadata=%s", key, entity, metadata);
         }
      }

      @Override
      public Void call() throws Exception {
         try {
            final MarshalledEntry marshalledEntry = marshallerEntryFactory.newMarshalledEntry(key, entity, metadata);
            if (marshalledEntry != null) {
               task.processEntry(marshalledEntry, taskContext);
            }
            return null;
         } catch (Exception e) {
            log.errorExecutingParallelStoreTask(e);
            throw e;
         }
      }
   }

   private class LoadingProcessTask implements Callable<Void> {
      private final CacheLoaderTask task;
      private final TaskContext taskContext;
      private final Object key;
      private final boolean fetchValue;
      private final boolean fetchMetadata;

      private LoadingProcessTask(CacheLoaderTask task, TaskContext taskContext, Object key, boolean fetchValue, boolean fetchMetadata) {
         this.task = task;
         this.taskContext = taskContext;
         this.key = key;
         this.fetchValue = fetchValue;
         this.fetchMetadata = fetchMetadata;
         if (trace) {
            log.tracef("Created process task with key=%s, fetchMetadata=%s", key, fetchMetadata);
         }
      }

      @Override
      public Void call() throws Exception {
         boolean loaded = false;
         Object entity;
         InternalMetadata metadata;

         // The loading of entries and metadata is offloaded to another thread.
         // We need second entity manager anyway because with MySQL we can't do streaming
         // in parallel with other queries using single connection
         EntityManager emExec = emf.createEntityManager();
         try {
            EntityTransaction txExec = emExec.getTransaction();
            txExec.begin();
            try {
               do {
                  try {
                     metadata = fetchMetadata ? getMetadata(emExec, key) : null;
                     if (trace) {
                        log.tracef("Fetched metadata (fetching? %s) %s", fetchMetadata, metadata);
                     }
                     if (metadata != null && metadata.isExpired(timeService.wallClockTime())) {
                        return null;
                     }
                     if (fetchValue) {
                        entity = findEntity(emExec, key);
                        if (trace) {
                           log.tracef("Fetched value %s", entity);
                        }
                     } else {
                        entity = null;
                     }
                  } finally {
                     try {
                        txExec.commit();
                        loaded = true;
                     } catch (Exception e) {
                        log.trace("Failed to load once", e);
                     }
                  }
               } while (!loaded);
            } finally {
               if (txExec != null && txExec.isActive()) {
                  txExec.rollback();
               }

            }
         } finally {
            if (emExec != null) {
               emExec.close();
            }
         }
         try {
            final MarshalledEntry marshalledEntry = marshallerEntryFactory.newMarshalledEntry(key, entity, metadata);
            if (marshalledEntry != null) {
               task.processEntry(marshalledEntry, taskContext);
            }
            return null;
         } catch (Exception e) {
            log.errorExecutingParallelStoreTask(e);
            throw e;
         }
      }
   }
}
