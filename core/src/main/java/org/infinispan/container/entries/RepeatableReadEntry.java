package org.infinispan.container.entries;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.container.entries.ReadCommittedEntry.Flags.COPIED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.SKIP_REMOTE_GET;

/**
 * An extension of {@link ReadCommittedEntry} that provides Repeatable Read semantics
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RepeatableReadEntry extends ReadCommittedEntry {
   private static final Log log = LogFactory.getLog(RepeatableReadEntry.class);

   public RepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
   }

   @Override
   public void copyForUpdate(DataContainer container) {
      if (isFlagSet(COPIED)) return; // already copied

      setFlag(COPIED); //mark as copied

      // make a backup copy
      oldValue = value;
   }

   public void performLocalWriteSkewCheck(DataContainer container, boolean alreadyCopied) {
      // check for write skew.
      InternalCacheEntry ice = container.get(key);

      Object actualValue = ice == null ? null : ice.getValue();
      Object valueToCompare = alreadyCopied ? oldValue : value;
      if (log.isTraceEnabled()) {
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
   public boolean isNull() {
      return value == null;
   }

   @Override
   public void setSkipRemoteGet(boolean skipRemoteGet) {
      setFlag(skipRemoteGet, SKIP_REMOTE_GET);
   }

   @Override
   public boolean skipRemoteGet() {
      return isFlagSet(SKIP_REMOTE_GET);
   }
}
