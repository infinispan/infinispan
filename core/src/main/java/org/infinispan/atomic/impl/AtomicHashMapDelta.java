package org.infinispan.atomic.impl;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Changes that have occurred on an AtomicHashMap
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class AtomicHashMapDelta implements Delta {
   private static final Log log = LogFactory.getLog(AtomicHashMapDelta.class);
   private static final boolean trace = log.isTraceEnabled();

   private List<Operation<Object, Object>> changeLog;
   private boolean hasClearOperation;

   @Override
   public DeltaAware merge(DeltaAware d) {
      AtomicHashMap<Object, Object> other;
      if (d != null && (d instanceof AtomicHashMap))
         other = (AtomicHashMap<Object, Object>) d;
      else
         other = new AtomicHashMap();
      if (changeLog != null) {
         for (Operation<Object, Object> o : changeLog) o.replay(other.delegate);
      }
      return other;
   }

   public void addOperation(Operation<?, ?> o) {
      if (changeLog == null) {
         // lazy init
         changeLog = new LinkedList<Operation<Object, Object>>();
      }
      if(o instanceof ClearOperation) {
         hasClearOperation = true;
      }

      if (trace)
         log.tracef("Add operation %s to delta", o);

      changeLog.add((Operation<Object, Object>) o);
   }

   public Collection<Object> getKeys() {
      List<Object> keys = new LinkedList<Object>();
      if (changeLog != null) {
         for (Operation<?, ?> o : changeLog) {
            Object key = o.keyAffected();
            keys.add(key);
         }
      }
      return keys;
   }

   public boolean hasClearOperation(){
      return hasClearOperation;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder( "AtomicHashMapDelta{changeLog=");
      sb.append(changeLog);
      sb.append( ",hasClear=");
      sb.append(hasClearOperation);
      sb.append("}");
      return sb.toString();
   }

   public int getChangeLogSize() {
      return changeLog == null ? 0 : changeLog.size();
   }

   public static class Externalizer extends AbstractExternalizer<AtomicHashMapDelta> {
      @Override
      public void writeObject(ObjectOutput output, AtomicHashMapDelta delta) throws IOException {
         if (trace) log.tracef("Serializing changeLog %s", delta.changeLog);
         output.writeObject(delta.changeLog);
      }

      @Override
      public AtomicHashMapDelta readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         AtomicHashMapDelta delta = new AtomicHashMapDelta();
         delta.changeLog = (List<Operation<Object, Object>>) input.readObject();
         if (trace) log.tracef("Deserialized changeLog %s", delta.changeLog);
         return delta;
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_HASH_MAP_DELTA;
      }

      @Override
      public Set<Class<? extends AtomicHashMapDelta>> getTypeClasses() {
         return Util.<Class<? extends AtomicHashMapDelta>>asSet(AtomicHashMapDelta.class);
      }
   }
}