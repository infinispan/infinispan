package org.infinispan.container.entries;

import static org.infinispan.container.entries.ReadCommittedEntry.Flags.SKIP_LOOKUP;

import org.infinispan.metadata.Metadata;
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

   /* Value before the last modification. Serves as the previous value when the operation is retried */
   protected Object oldValue;
   protected Metadata oldMetadata;

   public RepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
      this.oldValue = value;
      this.oldMetadata = metadata;
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
      metadata = oldMetadata;
   }

   @Override
   public void updatePreviousValue() {
      oldValue = value;
      oldMetadata = metadata;
   }

   public Object getOldValue() {
      return oldValue;
   }

   public Metadata getOldMetadata() {
      return oldMetadata;
   }

   public void setRead() {
      setFlag(Flags.READ);
   }

   public boolean isRead() {
      return isFlagSet(Flags.READ);
   }
}
