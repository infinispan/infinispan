package org.infinispan.persistence.jpa;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.GeneratedValue;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
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

   @Override
   public void clear() {
      EntityManager em = emf.createEntityManager();
      EntityTransaction txn = em.getTransaction();

      try {
         // the clear operation often deadlocks - let's try several times
         for (int i = 0;; ++i) {
            txn.begin();
            try {
               log.trace("Clearing JPA Store");
               String entityTable = em.getMetamodel().entity(configuration.entityClass()).getName();
               @SuppressWarnings("unchecked") List<Object> items = em.createQuery("FROM " + entityTable).getResultList();
               for(Object o : items)
                  em.remove(o);
               if (configuration.storeMetadata()) {
                  String metadataTable = em.getMetamodel().entity(MetadataEntity.class).getName();
                  Query clearMetadata = em.createQuery("DELETE FROM " + metadataTable);
                  clearMetadata.executeUpdate();
               }
               txn.commit();
               em.clear();
               break;
            } catch (Exception e) {
               log.trace("Failed to clear store", e);
               if (i >= 10) throw new JpaStoreException("Exception caught in clear()", e);
            } finally {
               if (txn != null && txn.isActive())
                  txn.rollback();
            }
         }
      } finally {
         em.close();
      }

   }

   public boolean delete(Object key) {
      if (!isValidKeyType(key)) {
         return false;
      }

      EntityManager em = emf.createEntityManager();
      try {
         long entityFindBegin = timeService.time();
         Object entity = em.find(configuration.entityClass(), key);
         stats.addEntityFind(timeService.time() - entityFindBegin);
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
            long metadataFindBegin = timeService.time();
            metadata = em.find(MetadataEntity.class, keyBytes);
            stats.addMetadataFind(timeService.time() - metadataFindBegin);
         }

         EntityTransaction txn = em.getTransaction();
         if (trace) log.trace("Removing " + entity + "(" + toString(metadata) + ")");
         long txnBegin = timeService.time();
         txn.begin();
         try {
            long entityRemoveBegin = timeService.time();
            em.remove(entity);
            stats.addEntityRemove(timeService.time() - entityRemoveBegin);
            if (metadata != null) {
               long metadataRemoveBegin = timeService.time();
               em.remove(metadata);
               stats.addMetadataRemove(timeService.time() - metadataRemoveBegin);
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

               long entityMergeBegin = timeService.time();
               em.merge(entity);
               stats.addEntityMerge(timeService.time() - entityMergeBegin);
               if (metadata != null && metadata.hasBytes()) {
                  long metadataMergeBegin = timeService.time();
                  em.merge(metadata);
                  stats.addMetadataMerge(timeService.time() - metadataMergeBegin);
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
            long entityFindBegin = timeService.time();
            Object entity = em.find(configuration.entityClass(), key);
            stats.addEntityFind(timeService.time() - entityFindBegin);
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
                  long metadataFindBegin = timeService.time();
                  MetadataEntity metadata = em.find(MetadataEntity.class, keyBytes);
                  stats.addMetadataFind(timeService.time() - metadataFindBegin);
                  if (trace) log.trace("Metadata " + key + " -> " + toString(metadata));
                  return metadata == null || metadata.expiration > timeService.wallClockTime();
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
            long entityFindBegin = timeService.time();
            Object entity = em.find(configuration.entityClass(), key);
            stats.addEntityFind(timeService.time() - entityFindBegin);
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
                  long metadataFindBegin = timeService.time();
                  MetadataEntity metadata = em.find(MetadataEntity.class, keyBytes);
                  stats.addMetadataFind(timeService.time() - metadataFindBegin);
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
   public void process(KeyFilter filter, final CacheLoaderTask task, Executor executor, boolean fetchValue, final boolean fetchMetadata) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      final TaskContextImpl taskContext = new TaskContextImpl();
      EntityManager em = emf.createEntityManager();

      try {
         CriteriaBuilder cb = em.getCriteriaBuilder();
         CriteriaQuery cq = cb.createQuery();
         Root root = cq.from(configuration.entityClass());
         Type idType = root.getModel().getIdType();
         SingularAttribute idAttr = root.getModel().getId(idType.getJavaType());
         cq.select(root.get(idAttr));

         for (final Object key : em.createQuery(cq).getResultList()) {
            if (taskContext.isStopped())
               break;
            if (filter != null && !filter.shouldLoadKey(key)) {
               if (trace) log.trace("Key " + key + " filtered");
               continue;
            }
            EntityTransaction txn = em.getTransaction();

            Object tempEntity = null;
            InternalMetadata tempMetadata = null;
            boolean loaded = false;
            txn.begin();
            try {
               do {
                  try {
                     tempEntity = fetchValue ? em.find(configuration.entityClass(), key) : null;
                     tempMetadata = fetchMetadata ? getMetadata(em, key) : null;
                  } finally {
                     try {
                        txn.commit();
                        loaded = true;
                     } catch (Exception e) {
                        log.trace("Failed to load once", e);
                     }
                  }
               } while (!loaded);
            } finally {
               if (txn != null && txn.isActive())
                  txn.rollback();
            }
            final Object entity = tempEntity;
            final InternalMetadata metadata = tempMetadata;
            if (trace) log.trace("Processing " + key + " -> " + entity + "(" + metadata + ")");

            if (metadata != null && metadata.isExpired(timeService.wallClockTime())) continue;
            eacs.submit(new Callable<Void>() {
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
            });
         }
         eacs.waitUntilAllCompleted();
         if (eacs.isExceptionThrown()) {
            throw new org.infinispan.persistence.spi.PersistenceException("Execution exception!", eacs.getFirstException());
         }
      } finally {
         em.close();
      }
   }

   private InternalMetadata getMetadata(EntityManager em, Object key) {
      byte[] keyBytes;
      try {
         keyBytes = marshaller.objectToByteBuffer(key);
      } catch (Exception e) {
         throw new JpaStoreException("Failed to marshall key", e);
      }
      MetadataEntity m = em.find(MetadataEntity.class, keyBytes);
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
      try {
         CriteriaBuilder builder = em.getCriteriaBuilder();
         CriteriaQuery<Long> cq = builder.createQuery(Long.class);
         cq.select(builder.count(cq.from(configuration.entityClass())));
         return em.createQuery(cq).getSingleResult().intValue();
      } finally {
         em.close();
      }
   }

   @Override
   public void purge(Executor threadPool, final PurgeListener listener) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(threadPool);
      EntityManager em = emf.createEntityManager();
      try {
         CriteriaBuilder cb = em.getCriteriaBuilder();
         CriteriaQuery<MetadataEntity> cq = cb.createQuery(MetadataEntity.class);

         Root root = cq.from(MetadataEntity.class);
         long currentTime = timeService.wallClockTime();
         cq.where(cb.le(root.get(MetadataEntity.EXPIRATION), currentTime));

         for (MetadataEntity metadata : em.createQuery(cq).getResultList()) {
            EntityTransaction txn = em.getTransaction();
            final Object key;
            try {
               key = marshaller.objectFromByteBuffer(metadata.name);
            } catch (Exception e) {
               throw new JpaStoreException("Cannot unmarshall key", e);
            }
            long txnBegin = timeService.time();
            txn.begin();
            try {
               long metadataFindBegin = timeService.time();
               metadata = em.find(MetadataEntity.class, metadata.name);
               stats.addMetadataFind(timeService.time() - metadataFindBegin);
               // check for transaction - I hope write skew check is done here
               if (metadata.expiration > currentTime) {
                  txn.rollback();
                  continue;
               }
               long entityFindBegin = timeService.time();
               Object entity = em.find(configuration.entityClass(), key);
               stats.addEntityFind(timeService.time() - entityFindBegin);
               if (entity != null) { // the entry may have been removed
                  long entityRemoveBegin = timeService.time();
                  em.remove(entity);
                  stats.addEntityRemove(timeService.time() - entityRemoveBegin);
               }
               long metadataRemoveBegin = timeService.time();
               em.remove(metadata);
               stats.addMetadataRemove(timeService.time() - metadataRemoveBegin);
               txn.commit();
               stats.addRemoveTxCommitted(timeService.time() - txnBegin);

               if (trace) log.trace("Expired " + key + " -> " + entity + "(" + toString(metadata) + ")");
               if (listener != null) {
                  eacs.submit(new Runnable() {
                     @Override
                     public void run() {
                        listener.entryPurged(key);
                     }
                  }, null);
               }
            } catch (RuntimeException e) {
               stats.addRemoveTxFailed(timeService.time() - txnBegin);
               throw e;
            } finally {
               if (txn != null && txn.isActive()) {
                  txn.rollback();
               }
            }
         }
      } finally {
         em.close();
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new JpaStoreException(eacs.getFirstException());
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
}
