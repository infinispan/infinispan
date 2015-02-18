package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.LockingConfiguration.*;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class LockingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<LockingConfiguration> {

   private static final Log log = LogFactory.getLog(LockingConfigurationBuilder.class);

   private final AttributeSet attributes;

   protected LockingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = LockingConfiguration.attributeDefinitionSet();
   }

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent
    * threads interacting with Infinispan. Similar to the concurrencyLevel tuning parameter seen in
    * the JDK's ConcurrentHashMap.
    */
   public LockingConfigurationBuilder concurrencyLevel(int i) {
      attributes.attribute(CONCURRENCY_LEVEL).set(i);
      return this;
   }

   /**
    * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation
    * levels. See <a href=
    * 'http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a
    * > for a discussion on isolation levels.
    */
   public LockingConfigurationBuilder isolationLevel(IsolationLevel isolationLevel) {
      attributes.attribute(ISOLATION_LEVEL).set(isolationLevel);
      return this;
   }

   /**
    * @see org.infinispan.configuration.cache.LockingConfiguration#supportsConcurrentUpdates()
    * @deprecated
    */
   @Deprecated
   public LockingConfigurationBuilder supportsConcurrentUpdates(boolean itDoes) {
      if (!itDoes) {
         log.warnConcurrentUpdateSupportCannotBeConfigured();
      }
      return this;
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public LockingConfigurationBuilder lockAcquisitionTimeout(long l) {
      attributes.attribute(LOCK_ACQUISITION_TIMEOUT).set(l);
      return this;
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public LockingConfigurationBuilder lockAcquisitionTimeout(long l, TimeUnit unit) {
      return lockAcquisitionTimeout(unit.toMillis(l));
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked.
    * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
    * footprint but may reduce concurrency in the system.
    */
   public LockingConfigurationBuilder useLockStriping(boolean b) {
      attributes.attribute(USE_LOCK_STRIPING).set(b);
      return this;
   }

   /**
    * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is set
    * to false, if the writer at commit time discovers that the working entry and the underlying
    * entry have different versions, the working entry will overwrite the underlying entry. If true,
    * such version conflict - known as a write-skew - will throw an Exception.
    */
   public LockingConfigurationBuilder writeSkewCheck(boolean b) {
      attributes.attribute(WRITE_SKEW_CHECK).set(b);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(WRITE_SKEW_CHECK).get()) {
         if (attributes.attribute(ISOLATION_LEVEL).get() != IsolationLevel.REPEATABLE_READ)
            throw new CacheConfigurationException("Write-skew checking only allowed with REPEATABLE_READ isolation level for cache");
         if (transaction().lockingMode() != LockingMode.OPTIMISTIC)
            throw new CacheConfigurationException("Write-skew checking only allowed with OPTIMISTIC transactions");
         if (!versioning().enabled() || versioning().scheme()!= VersioningScheme.SIMPLE)
            throw new CacheConfigurationException(
                  "Write-skew checking requires versioning to be enabled and versioning scheme 'SIMPLE' to be configured");
         if (clustering().cacheMode() != CacheMode.DIST_SYNC && clustering().cacheMode() != CacheMode.REPL_SYNC
               && clustering().cacheMode() != CacheMode.LOCAL)
            throw new CacheConfigurationException("Write-skew checking is only supported in REPL_SYNC, DIST_SYNC and LOCAL modes.  "
                  + clustering().cacheMode() + " cannot be used with write-skew checking");
      }

      Attribute<IsolationLevel> isolationLevel = attributes.attribute(ISOLATION_LEVEL);
      if (getBuilder().clustering().cacheMode().isClustered() && isolationLevel.get() == IsolationLevel.NONE)
         isolationLevel.set(IsolationLevel.READ_COMMITTED);

      if (isolationLevel.get() == IsolationLevel.READ_UNCOMMITTED)
         isolationLevel.set(IsolationLevel.READ_COMMITTED);

      if (isolationLevel.get() == IsolationLevel.SERIALIZABLE)
         isolationLevel.set(IsolationLevel.REPEATABLE_READ);
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public LockingConfiguration create() {
      return new LockingConfiguration(attributes.protect());
   }

   @Override
   public LockingConfigurationBuilder read(LockingConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + "[" + attributes + "]";
   }
}
