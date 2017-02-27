package org.infinispan.container.entries;

import static org.infinispan.container.entries.ReadCommittedEntry.Flags.SKIP_LOOKUP;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An extension of {@link ReadCommittedEntry} that provides Repeatable Read semantics
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RepeatableReadEntry extends ReadCommittedEntry {
   private static final Log log = LogFactory.getLog(RepeatableReadEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   /* The value before we start modifying the entry (after first read in transaction) */
   protected Object initialValue;
   /* Value before the last modification. Serves as the previous value when the operation is retried */
   protected Object oldValue;

   public RepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
      this.initialValue = value;
      this.oldValue = value;
   }

   // TODO: ISPN-7527 reference-based check is insufficient if we're using persistence
   public void performLocalWriteSkewCheck(DataContainer container) {
      // check for write skew.
      InternalCacheEntry ice = container.get(key);

      Object actualValue = ice == null ? null : ice.getValue();
      Object valueToCompare = initialValue;
      if (trace) {
         log.tracef("Performing local write skew check. actualValue=%s, transactionValue=%s", actualValue, valueToCompare);
      }
      // Note that this identity-check is intentional.  We don't *want* to call actualValue.equals() since that defeats the purpose.
      // the implicit "versioning" we have in R_R creates a new wrapper "value" instance for every update.
      if (actualValue != null && actualValue != valueToCompare) {
         log.unableToCopyEntryForUpdate(getKey());
         throw new WriteSkewException("Detected write skew.", key);
      }

      if (valueToCompare != null && ice == null && !isCreated()) {
         // We still have a write-skew here.  When this wrapper was created there was an entry in the data container
         // (hence isCreated() == false) but 'ice' is now null.
         log.unableToCopyEntryForUpdate(getKey());
         throw new WriteSkewException("Detected write skew - concurrent removal of entry!", key);
      }
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      setFlag(skipLookup, SKIP_LOOKUP);
   }

   @Override
   public boolean skipLookup() {
      return isFlagSet(SKIP_LOOKUP);
   }

   @Override
   public RepeatableReadEntry clone() {
      return (RepeatableReadEntry) super.clone();
   }

   @Override
   public final Object setValue(Object value) {
      super.setValue(value);
      setSkipLookup(true);
      return oldValue;
   }

   @Override
   public void resetCurrentValue() {
      value = oldValue;
   }

   @Override
   public void updatePreviousValue() {
      oldValue = value;
   }

   @Override
   public void updateInitialValue(Object value) {
      assert !skipLookup();
      initialValue = value;
   }

   public void setRead() {
      setFlag(Flags.READ);
   }

   public boolean isRead() {
      return isFlagSet(Flags.READ);
   }
}
