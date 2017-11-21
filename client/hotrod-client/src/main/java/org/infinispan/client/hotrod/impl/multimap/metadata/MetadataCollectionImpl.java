package org.infinispan.client.hotrod.impl.multimap.metadata;

import java.util.Collection;

import org.infinispan.client.hotrod.multimap.MetadataCollection;

/**
 * The values used in this class are assumed to be in MILLISECONDS
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class MetadataCollectionImpl<V> implements MetadataCollection<V> {
   private final Collection<V> collection;
   private final long created;
   private final int lifespan;
   private final long lastUsed;
   private final int maxIdle;
   private final long version;

   public MetadataCollectionImpl(Collection<V> collection) {
      this.collection = collection;
      this.created = -1;
      this.lifespan = -1;
      this.lastUsed = -1;
      this.maxIdle = -1;
      this.version = -1;
   }

   public MetadataCollectionImpl(Collection<V> collection, long created, int lifespan, long lastUsed, int maxIdle, long version) {
      this.collection = collection;
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
      this.version = version;
   }

   @Override
   public Collection<V> getCollection() {
      return collection;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public int getLifespan() {
      return lifespan;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public int getMaxIdle() {
      return maxIdle;
   }

   @Override
   public long getVersion() {
      return version;
   }

   @Override
   public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("MetadataCollectionImpl{");
      b.append("version=");
      b.append(version);
      b.append(", created=");
      b.append(created);
      b.append(", lastUsed=");
      b.append(lastUsed);
      b.append(", lifespan=");
      b.append(lifespan);
      b.append(", maxIdle=");
      b.append(maxIdle);
      b.append(", collection=");
      b.append(collection);
      b.append("}");
      return b.toString();
   }
}
