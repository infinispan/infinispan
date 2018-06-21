package org.infinispan.atomic.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.atomic.FineGrainedAtomicMap;
import org.infinispan.container.impl.MergeOnStore;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.functional.EntryView;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.group.Group;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class is expected to be modified without locking, and results merged using the {@link #merge(Object)} method
 * when committing to DC. Also, the keys are not persisted, we record only existence of this map and then upon load
 * we retrieve the keys through {@link GetKeysInGroupCommand}.
 *
 * On transactional cache it is safe to execute concurrent adds/removes to this class even without the lock
 * because we'll never prepare-commit another transaction that would modify the same key concurrently - the transaction
 * will be synchronized on the modified key (which acquires locks properly). This is kind of piggybacking on other key's
 * locking scheme.
 *
 * The only exception to that rule is when we attempt to remove this map completely - this is not safe to proceed
 * concurrently to any other modification.
 */
public final class AtomicKeySetImpl<K> implements MergeOnStore {
   private enum Type {
      KEY, READ_ALL, TOUCH, ADD, ADD_ALL, REMOVE, REMOVE_ALL, REMOVE_MAP
   }

   private static final Type[] TYPES = Type.values();
   private static Log log = LogFactory.getLog(FineGrainedAtomicMap.class);

   private final ByteString cacheName;
   private final Object group;
   // added and keys should be distinct
   private final transient Collection<K> added;
   private final transient Collection<K> removed;
   private final transient Set<K> keys;

   static <K> AtomicKeySetImpl<K> create(String cacheName, Object group, Set<K> keys) {
      return new AtomicKeySetImpl<>(ByteString.fromString(cacheName), group, keys, null, null);
   }

   private AtomicKeySetImpl(ByteString cacheName, Object group, Set<K> keys, Collection<K> added, Collection<K> removed) {
      this.cacheName = cacheName;
      this.group = group;
      this.keys = keys;
      this.added = added;
      this.removed = removed;
   }

   @Override
   public Object merge(Object other) {
      Set<K> actual = null;
      if (other != null) {
         if (other.getClass() != AtomicKeySetImpl.class) {
            throw log.atomicMapHasWrongType(other, AtomicKeySetImpl.class);
         }
         actual = ((AtomicKeySetImpl) other).keys;
      }
      Set<K> keys = new HashSet<>((actual == null ? 0 : actual.size())
            + (added == null ? 0 : added.size()) + (removed == null ? 0 : removed.size()));
      if (actual != null) keys.addAll(actual);
      if (added != null) keys.addAll(added);
      if (removed != null) keys.addAll(removed);
      return new AtomicKeySetImpl<>(cacheName, group, keys, null, null);
   }

   private Set<K> toSet() {
      if (removed == null && added == null) {
         return keys;
      }
      HashSet<K> set = new HashSet<>();
      set.addAll(keys);
      if (removed != null) set.removeAll(removed);
      if (added != null) set.addAll(added);
      return set;
   }

   @Override
   public String toString() {
      return "AtomicKeySetImpl{keys=" + keys + ", added=" + added + ", removed=" + removed + "}";
   }

   interface Externalizable {
      Type type();
      void writeTo(ObjectOutput output) throws IOException;
   }

   final static class Key<MK, K> implements Externalizable {
      private final MK group;
      private final K key;

      Key(MK group, K key) {
         this.group = group;
         this.key = key;
      }

      public static <MK, K> Key<MK, K> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Key<>((MK) input.readObject(), (K) input.readObject());
      }

      @Group
      public MK group() {
         return group;
      }

      public K key() {
         return key;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Key<?, ?> key1 = (Key<?, ?>) o;

         if (!group.equals(key1.group)) return false;
         return key.equals(key1.key);
      }

      @Override
      public int hashCode() {
         int result = group.hashCode();
         result = 31 * result + key.hashCode();
         return result;
      }

      @Override
      public Type type() {
         return Type.KEY;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         output.writeObject(group);
         output.writeObject(key);
      }

      @Override
      public String toString() {
         return "AtomicKeySetImpl.Key{group=" + group + ", key=" + key + "}";
      }
   }

   static class ReadAll<K> implements Function<EntryView.ReadEntryView<Object, Object>, Set<K>>, Externalizable {
      private static final ReadAll INSTANCE = new ReadAll();

      static <K> ReadAll<K> instance() {
         return INSTANCE;
      }

      @Override
      public Set<K> apply(EntryView.ReadEntryView<Object, Object> view) {
         return view.find().map(value -> {
            if (value instanceof AtomicKeySetImpl) {
               return ((AtomicKeySetImpl) value).toSet();
            } else {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         }).orElse(null);
      }

      @Override
      public Type type() {
         return Type.READ_ALL;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
      }
   }

   static class Touch<MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Void>, Externalizable {
      private final ByteString cacheName;

      public Touch(ByteString cacheName) {
         this.cacheName = cacheName;
      }

      public static <MK> Touch readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         ByteString cacheName = ByteString.readObject(input);
         return new Touch(cacheName);
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            // Trying to touch when the context contains a delta shouldn't pose a problem
            if (!(value instanceof AtomicKeySetImpl)) {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         } else {
            view.set(new AtomicKeySetImpl(cacheName, view.key(), Collections.emptySet(), null, null));
         }
         return null;
      }

      @Override
      public Type type() {
         return Type.TOUCH;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         ByteString.writeObject(output, cacheName);
      }
   }

   static class Add<K> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, Externalizable {
      private final K key;

      public Add(K key) {
         this.key = key;
      }

      public static <K> Add readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Add((K) input.readObject());
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            if (value instanceof AtomicKeySetImpl) {
               AtomicKeySetImpl<K> set = (AtomicKeySetImpl<K>) value;
               if (set.removed != null && set.removed.contains(key)) {
                  Collection<K> removed = set.removed.stream().filter(k -> !key.equals(k)).collect(Collectors.toList());
                  view.set(new AtomicKeySetImpl<>(set.cacheName, set.group, set.keys, set.added, removed));
               } else {
                  Collection<K> added;
                  if (set.added == null) {
                     added = Collections.singleton(key);
                  } else if (set.added.contains(key)) {
                     return null;
                  } else {
                     added = new ArrayList<>(set.added.size() + 1);
                     added.addAll(set.added);
                     added.add(key);
                  }
                  view.set(new AtomicKeySetImpl<>(set.cacheName, set.group, set.keys, added, set.removed));
               }
               return null;
            } else {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         } else {
            throw log.atomicMapDoesNotExist();
         }
      }

      @Override
      public Type type() {
         return Type.ADD;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         output.writeObject(key);
      }
   }

   static class AddAll<K> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, Externalizable {
      private final Collection<? extends K> keys;

      public AddAll(Collection<? extends K> keys) {
         this.keys = keys;
      }

      public static <K> AddAll readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         ArrayList<K> keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
         return new AddAll(keys);
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            if (value instanceof AtomicKeySetImpl) {
               AtomicKeySetImpl<K> set = (AtomicKeySetImpl<K>) value;
               Collection<K> removed = set.removed;
               Collection<K> added = set.added;
               for (K key : keys) {
                  if (removed != null && removed.contains(key)) {
                     if (removed == set.removed) {
                        removed = removed.stream().filter(k -> !key.equals(k)).collect(Collectors.toCollection(ArrayList::new));
                     } else {
                        removed.remove(key);
                     }
                  } else {
                     if (added == null) {
                        added = new ArrayList<>(keys.size());
                     } else if (added == set.added) {
                        added = new ArrayList<>(set.added.size() + keys.size());
                     }
                     added.add(key);
                  }
               }
               view.set(new AtomicKeySetImpl<>(set.cacheName, set.group, set.keys, added, removed));
               return null;
            } else {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         } else {
            throw log.atomicMapDoesNotExist();
         }
      }

      @Override
      public Type type() {
         return Type.ADD_ALL;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         MarshallUtil.marshallCollection(keys, output);
      }
   }

   static class Remove<K> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, Externalizable {
      private final K key;

      public Remove(K key) {
         this.key = key;
      }

      public static <K> Remove readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Remove((K) input.readObject());
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            if (value instanceof AtomicKeySetImpl) {
               AtomicKeySetImpl<K> set = (AtomicKeySetImpl<K>) value;
               if (set.added != null && set.added.contains(key)) {
                  Collection<K> added = set.added.stream().filter(k -> !key.equals(k)).collect(Collectors.toList());
                  view.set(new AtomicKeySetImpl<>(set.cacheName, view.key(), set.keys, added, set.removed));
               } else {
                  Collection<K> removed;
                  if (set.removed == null) {
                     removed = Collections.singleton(key);
                  } else if (set.removed.contains(key)) {
                     return null;
                  } else {
                     removed = new ArrayList<>(set.removed.size() + 1);
                     removed.addAll(set.removed);
                     removed.add(key);
                  }
                  view.set(new AtomicKeySetImpl<>(set.cacheName, view.key(), set.keys, set.added, removed));
               }
               return null;
            } else {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         } else {
            throw log.atomicMapDoesNotExist();
         }
      }

      @Override
      public Type type() {
         return Type.REMOVE;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         output.writeObject(key);
      }
   }

   static class RemoveAll<K> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Set<K>>, Externalizable {
      private static final RemoveAll INSTANCE = new RemoveAll();

      public static <K> RemoveAll<K> instance() {
         return INSTANCE;
      }

      @Override
      public Set<K> apply(EntryView.ReadWriteEntryView<Object, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            if (value instanceof AtomicKeySetImpl) {
               AtomicKeySetImpl<K> set = (AtomicKeySetImpl<K>) value;
               HashSet<K> currentlyRemoved = new HashSet<>();
               currentlyRemoved.addAll(set.keys);
               if (set.added != null) currentlyRemoved.addAll(set.added);
               HashSet<K> removed = new HashSet<>(currentlyRemoved);
               if (set.removed != null) removed.addAll(set.removed);
               view.set(new AtomicKeySetImpl<>(set.cacheName, set.group, Collections.emptySet(), null, removed));
               return currentlyRemoved;
            } else {
               throw log.atomicMapHasWrongType(value, AtomicKeySetImpl.class);
            }
         } else {
            throw log.atomicMapDoesNotExist();
         }
      }

      @Override
      public Type type() {
         return Type.REMOVE_ALL;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
      }
   }

   static class RemoveMap<K> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Set<K>>, Externalizable {
      private static final RemoveMap INSTANCE = new RemoveMap();

      public static <K> RemoveMap<K> instance() {
         return INSTANCE;
      }

      @Override
      public Set<K> apply(EntryView.ReadWriteEntryView<Object, Object> view) {
         if (view.find().isPresent()) {
            Object value = view.find().get();
            view.remove();
            if (value instanceof AtomicKeySetImpl) {
               AtomicKeySetImpl<K> set = (AtomicKeySetImpl<K>) value;
               return set.toSet();
            }
         }
         return null;
      }

      @Override
      public Type type() {
         return Type.REMOVE_MAP;
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
      }
   }

   public static class Externalizer implements AdvancedExternalizer<AtomicKeySetImpl> {
      private final GlobalComponentRegistry gcr;

      public Externalizer(GlobalComponentRegistry gcr) {
         this.gcr = gcr;
      }

      @Override
      public Set<Class<? extends AtomicKeySetImpl>> getTypeClasses() {
         return Util.asSet(AtomicKeySetImpl.class);
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_KEY_SET;
      }

      @Override
      public void writeObject(ObjectOutput output, AtomicKeySetImpl object) throws IOException {
         ByteString.writeObject(output, object.cacheName);
         output.writeObject(object.group);
      }

      @Override
      public AtomicKeySetImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ByteString cacheName1 = ByteString.readObject(input);
         Object group = input.readObject();
         ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName1);
         InvocationContextFactory icf = cr.getComponent(InvocationContextFactory.class);
         InvocationContext ctx = icf.createNonTxInvocationContext();
         AsyncInterceptorChain chain = cr.getComponent(AsyncInterceptorChain.class);
         GetKeysInGroupCommand cmd = cr.getCommandsFactory().buildGetKeysInGroupCommand(0, group);
         Map<Object, Object> map = (Map<Object, Object>) chain.invoke(ctx, cmd);
         // GetKeyInGroupCommand will set skip lookup for all entries in context, including the entry which
         // is currently being loaded. We have to revert the setting or it won't be loaded.
         CacheEntry entry = ctx.lookupEntry(group);
         if (entry != null) {
            entry.setSkipLookup(false);
         }
         // In a similar fashion the GetKeyInGroupCommand recorded version seen before it was loaded
         // it recorded it as a non-existent version.
         // The versions read map is not immutable, though, so we can undo that operation.
         if (ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).getCacheTransaction().getVersionsRead().remove(group);
         }
         return new AtomicKeySetImpl(cacheName1, group, map.keySet(), null, null);
      }
   }

   public static class FunctionExternalizer implements AdvancedExternalizer<Externalizable> {
      @Override
      public Set<Class<? extends Externalizable>> getTypeClasses() {
         return Util.asSet(Key.class, ReadAll.class, Touch.class, Add.class, AddAll.class, Remove.class, RemoveAll.class, RemoveMap.class);
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_FINE_GRAINED_MAP_FUNCTIONS;
      }

      @Override
      public void writeObject(ObjectOutput output, Externalizable object) throws IOException {
         output.writeByte(object.type().ordinal());
         object.writeTo(output);
      }

      @Override
      public Externalizable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Type type = TYPES[input.readByte()];
         switch (type) {
            case KEY:
               return Key.readFrom(input);
            case READ_ALL:
               return ReadAll.instance();
            case TOUCH:
               return Touch.readFrom(input);
            case ADD:
               return Add.readFrom(input);
            case ADD_ALL:
               return AddAll.readFrom(input);
            case REMOVE:
               return Remove.readFrom(input);
            case REMOVE_ALL:
               return RemoveAll.instance();
            case REMOVE_MAP:
               return RemoveMap.instance();
            default:
               throw new IllegalArgumentException();
         }
      }
   }
}
