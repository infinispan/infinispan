package org.infinispan.persistence.jpa;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityTransaction;
import javax.persistence.GeneratedValue;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;
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
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.jpa.impl.EntityManagerFactoryRegistry;
import org.infinispan.persistence.jpa.impl.MetadataEntity;
import org.infinispan.persistence.jpa.impl.MetadataEntityKey;
import org.infinispan.persistence.jpa.impl.Stats;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.internal.functions.Functions;
import io.reactivex.schedulers.Schedulers;

/**
 * NOTE: This store can return expired keys or entries on any given operation if
 * {@link JpaStoreConfiguration#storeMetadata()} was set to false.
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Store(shared = true)
@ConfiguredBy(JpaStoreConfiguration.class)
public class JpaStore<K, V> implements AdvancedLoadWriteStore<K, V> {
   private static final Log log = LogFactory.getLog(JpaStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private JpaStoreConfiguration configuration;
   private EntityManagerFactory emf;
   private EntityManagerFactoryRegistry emfRegistry;
   private StreamingMarshaller marshaller;
   private MarshallableEntryFactory<K,V> marshallerEntryFactory;
   private TimeService timeService;
   private Scheduler scheduler;
   private Stats stats = new Stats();
   private boolean setFetchSizeMinInteger = false;

   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      this.emfRegistry = ctx.getCache().getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry().getComponent(EntityManagerFactoryRegistry.class);
      this.marshallerEntryFactory = ctx.getMarshallableEntryFactory();
      this.marshaller = ctx.getMarshaller();
      this.timeService = ctx.getTimeService();
      this.scheduler = Schedulers.from(ctx.getExecutor());
   }

   @Override
   public void start() {
      this.emf = emfRegistry.getEntityManagerFactory(configuration.persistenceUnitName());

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
         log.debug("JPA Store stopped, stats: " + stats);
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
         ArrayList<Object> batch = configuration.maxBatchSize() > 0 ? new ArrayList<>(configuration.maxBatchSize()) : new ArrayList<>();
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
               if (batch.size() == configuration.maxBatchSize()) {
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

   @Override
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
         MetadataEntity metadata = getMetadataEntity(key, em);
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
   public void deleteBatch(Iterable<Object> keys) {
      EntityManager em = emf.createEntityManager();
      try {
         EntityTransaction txn = em.getTransaction();
         long txnBegin = timeService.time();
         try {
            txn.begin();

            CriteriaBuilder cb = em.getCriteriaBuilder();
            CriteriaDelete query = cb.createCriteriaDelete(configuration.entityClass());
            Root root = query.from(configuration.entityClass());
            SingularAttribute id = getEntityId(em, configuration.entityClass());
            query.where(root.get(id).in(keys));
            em.createQuery(query).executeUpdate();

            if (configuration.storeMetadata()) {
               List<MetadataEntityKey> metaKeys = StreamSupport.stream(keys.spliterator(), false).map(this::getMetadataKey).collect(Collectors.toList());
               CriteriaDelete<MetadataEntity> metaQuery = cb.createCriteriaDelete(MetadataEntity.class);
               Root<MetadataEntity> metaRoot = metaQuery.from(MetadataEntity.class);
               id = getEntityId(em, MetadataEntity.class);
               query.where(metaRoot.get(id).in(metaKeys));
               em.createQuery(metaQuery).executeUpdate();
            }
            txn.commit();
            stats.addBatchRemoveTxCommitted(timeService.time() - txnBegin);
         } catch (Exception e) {
            stats.addBatchRemoveTxFailed(timeService.time() - txnBegin);
            if (e instanceof JpaStoreException)
               throw e;
            throw new JpaStoreException("Exception caught in deleteBatch()", e);
         } finally {
            if (txn != null && txn.isActive())
               txn.rollback();
         }
      } finally {
         em.close();
      }
   }

   private SingularAttribute getEntityId(EntityManager em, Class clazz) {
      Metamodel meta = em.getMetamodel();
      IdentifiableType identifiableType = (IdentifiableType) meta.managedType(clazz);
      return identifiableType.getId(identifiableType.getIdType().getJavaType());
   }

   private MetadataEntityKey getMetadataKey(Object key) {
      byte[] keyBytes;
      try {
         keyBytes = marshaller.objectToByteBuffer(key);
      } catch (Exception e) {
         throw new JpaStoreException("Failed to marshall key", e);
      }
      return new MetadataEntityKey(keyBytes);
   }

   private MetadataEntity getMetadataEntity(Object key, EntityManager em) {
      return configuration.storeMetadata() ? findMetadata(em, getMetadataKey(key)) : null;
   }

   @Override
   public void write(MarshallableEntry entry) {
      EntityManager em = emf.createEntityManager();

      Object entity = entry.getValue();
      MetadataEntity metadata = configuration.storeMetadata() ? new MetadataEntity(entry) : null;
      try {
         validateEntityIsAssignable(entity);
         validateObjectId(entry);
         EntityTransaction txn = em.getTransaction();

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
      } finally {
         em.close();
      }
   }

   @Override
   public CompletionStage<Void> bulkUpdate(Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      Flowable.using(() -> {
               EntityManager em = emf.createEntityManager();
               EntityTransaction txn = em.getTransaction();
               return new KeyValuePair<>(em, txn);
            },
            kvp -> createBatchFlowable(kvp.getKey(), kvp.getValue(), publisher),
            kvp -> {
               EntityTransaction txn = kvp.getValue();
               if (txn != null && txn.isActive())
                  txn.rollback();
               kvp.getKey().close();
            })
            .doOnError(e -> {
               if (e instanceof JpaStoreException)
                  throw (JpaStoreException) e;
               throw new JpaStoreException("Exception caught in bulkUpdate()", e);
            })
            .subscribe(Functions.emptyConsumer(), future::completeExceptionally, () -> future.complete(null));
      return future;
   }

   private Flowable<MarshallableEntry<? extends K, ? extends V>> createBatchFlowable(EntityManager em, EntityTransaction txn, Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
      final long txnBegin = timeService.time();
      txn.begin();
      return Flowable.fromPublisher(publisher)
            .doOnNext(entry -> {
               Object entity = entry.getValue();
               validateEntityIsAssignable(entity);
               validateObjectId(entry);

               MetadataEntity metadata = configuration.storeMetadata() ? new MetadataEntity(entry) : null;
               mergeEntity(em, entity);
               if (metadata != null && metadata.hasBytes())
                  mergeMetadata(em, metadata);
            })
            .doOnComplete(() -> {
               stats.addBatchWriteTxCommitted(timeService.time() - txnBegin);
               txn.commit();
            })
            .doOnError(e -> stats.addBatchWriteTxFailed(timeService.time() - txnBegin))
            .doFinally(() -> {
               if (txn.isActive())
                  txn.rollback();
            });
   }

   private void validateObjectId(MarshallableEntry entry) {
      Object id = emf.getPersistenceUnitUtil().getIdentifier(entry.getValue());
      if (!entry.getKey().equals(id)) {
         throw new JpaStoreException(
               "Entity id value must equal to key of cache entry: "
                     + "key = [" + entry.getKey() + "], id = ["
                     + id + "]");
      }
   }

   private void validateEntityIsAssignable(Object entity) {
      if (!configuration.entityClass().isAssignableFrom(entity.getClass())) {
         throw new JpaStoreException(String.format(
               "This cache is configured with JPA CacheStore to only store values of type %s - cannot write %s = %s",
               configuration.entityClass().getName(), entity, entity.getClass().getName()));
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
                  MetadataEntity metadata = findMetadata(em, getMetadataKey(key));
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
   public MarshallableEntry loadEntry(Object key) {
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
            if (entity == null)
               return null;

            try {
               MetadataEntity metaEntity = getMetadataEntity(key, em);
               if (metaEntity != null && metaEntity.getMetadata() != null) {
                  Metadata metadata;
                  try {
                     metadata = (Metadata) marshaller.objectFromByteBuffer(metaEntity.getMetadata());
                  } catch (Exception e) {
                     throw new JpaStoreException("Failed to unmarshall metadata", e);
                  }
                  if (isExpired(metaEntity)) {
                     return null;
                  }
                  if (trace) log.trace("Loaded " + entity + " (" + metadata + ")");
                  return marshallerEntryFactory.create(key, entity, metadata, metaEntity.getCreated(), metaEntity.getLastUsed());
               }
               if (trace) log.trace("Loaded " + entity);
               return marshallerEntryFactory.create(key, entity);
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
   public Flowable<K> publishKeys(Predicate<? super K> filter) {
      return Flowable.using(() -> {
         EntityManager emStream = emf.createEntityManager();
         Session session = emStream.unwrap(Session.class);
         Criteria criteria = session.createCriteria(configuration.entityClass()).setProjection(Projections.id()).setReadOnly(true);
         if (setFetchSizeMinInteger) {
            criteria.setFetchSize(Integer.MIN_VALUE);
         }
         ScrollableResults results = criteria.scroll(ScrollMode.FORWARD_ONLY);
         return new KeyValuePair<>(emStream, results);
      }, kvp -> {
         ScrollableResults results = kvp.getValue();
         return Flowable.fromIterable(() -> new ScrollableResultsIterator(results, filter)
         );
      }, kvp -> {
         try {
            kvp.getValue().close();
         } finally {
            kvp.getKey().close();
         }
      });
   }

   @Override
   public Flowable<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      boolean innerFetchMetadata;
      if (fetchMetadata && !configuration.storeMetadata()) {
         log.debug("Metadata cannot be retrieved as JPA Store is not configured to persist metadata.");
         innerFetchMetadata = false;
      } else {
         innerFetchMetadata = fetchMetadata;
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
      Flowable<K> keyPublisher = publishKeys(filter);

      if (fetchValue || innerFetchMetadata) {
         return keyPublisher
               // Run the loading in parallel using executor since it will be blocking
               .parallel()
               .runOn(scheduler)
               .map(k -> loadEntry(k, fetchValue, innerFetchMetadata))
               .sequential();
      } else {
         return keyPublisher.map(k -> marshallerEntryFactory.create(k));
      }
   }

   private boolean isExpired(MetadataEntity entity) {
      long expiry = entity.getExpiration();
      return expiry > 0 && expiry <= timeService.wallClockTime();
   }

   private Metadata getMetadata(MetadataEntity entity) {
      if (entity == null)
         return null;

      try {
         return (Metadata) marshaller.objectFromByteBuffer(entity.getMetadata());
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
            ArrayList<MetadataEntityKey> batch = configuration.maxBatchSize() > 0 ? new ArrayList<>(configuration.maxBatchSize()) : new ArrayList<>();
            while (metadataKeys.next()) {
               MetadataEntityKey mKey = (MetadataEntityKey) metadataKeys.get(0);
               batch.add(mKey);
               if (batch.size() == configuration.maxBatchSize()) {
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

   private MarshallableEntry<K, V> loadEntry(Object key, boolean fetchValue, boolean fetchMetadata) {
      Object entity;
      Metadata metadata;
      MetadataEntity metaEntity;

      // The loading of entries and metadata is offloaded to another thread.
      // We need second entity manager anyway because with MySQL we can't do streaming
      // in parallel with other queries using single connection
      EntityManager emExec = emf.createEntityManager();
      try {
         metaEntity = fetchMetadata ? getMetadataEntity(key, emExec) : null;
         metadata = getMetadata(metaEntity);
         if (trace) {
            log.tracef("Fetched metadata (fetching? %s) %s", fetchMetadata, metadata);
         }
         if (metaEntity != null && isExpired(metaEntity)) {
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
         if (emExec != null) {
            emExec.close();
         }
      }
      try {
         return metaEntity == null ?
               marshallerEntryFactory.create(key, entity) :
               marshallerEntryFactory.create(key, entity, metadata, metaEntity.getCreated(), metaEntity.getLastUsed());
      } catch (Exception e) {
         log.errorExecutingParallelStoreTask(e);
         throw e;
      }
   }

   private class ScrollableResultsIterator extends AbstractIterator<K> {
      private final ScrollableResults results;
      private final Predicate<? super K> filter;

      public ScrollableResultsIterator(ScrollableResults results, Predicate<? super K> filter) {
         this.results = results;
         this.filter = filter;
      }

      @Override
      protected K getNext() {
         K key = null;
         while (key == null && results.next()) {
            K testKey = (K) results.get(0);
            if (filter == null || filter.test(testKey)) {
               key = testKey;
            }
         }
         return key;
      }
   }
}
