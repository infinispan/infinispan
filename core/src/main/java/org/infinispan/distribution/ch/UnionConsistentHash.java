package org.infinispan.distribution.ch;

import org.infinispan.CacheException;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshalls;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A delegating wrapper that locates keys by getting a union of locations reported by two other ConsistentHash
 * implementations it delegates to.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnionConsistentHash extends AbstractConsistentHash {

   ConsistentHash oldCH, newCH;

   public UnionConsistentHash(ConsistentHash oldCH, ConsistentHash newCH) {
      if ((oldCH instanceof UnionConsistentHash) || (newCH instanceof UnionConsistentHash))
         throw new CacheException("Expecting both newCH and oldCH to not be Unions!!  oldCH=[" + oldCH.getClass() + "] and newCH=[" + newCH.getClass() + "]");
      this.oldCH = oldCH;
      this.newCH = newCH;
   }

   public void setCaches(List<Address> caches) {
      // no op
   }

   public List<Address> getCaches() {
      return Collections.emptyList();
   }

   public List<Address> locate(Object key, int replCount) {
      Set<Address> addresses = new LinkedHashSet<Address>();
      addresses.addAll(oldCH.locate(key, replCount));
      addresses.addAll(newCH.locate(key, replCount));
      return Immutables.immutableListConvert(addresses);
   }

   @Override
   public int getHashId(Address a) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   @Override
   public List<Address> getBackupsForNode(Address node, int replCount) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   @Override
   public int getHashSpace() {
      int oldHashSpace = oldCH.getHashSpace();
      int newHashSpace = newCH.getHashSpace();
      // In a union, the hash space is the biggest of the hash spaces.
      return oldHashSpace > newHashSpace ? oldHashSpace : newHashSpace;
   }

   public ConsistentHash getNewConsistentHash() {
      return newCH;
   }

   public ConsistentHash getOldConsistentHash() {
      return oldCH;
   }

   @Marshalls(typeClasses = UnionConsistentHash.class, id = Ids.UNION_CONSISTENT_HASH)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<UnionConsistentHash> {
      public void writeObject(ObjectOutput output, UnionConsistentHash uch) throws IOException {
         output.writeObject(uch.oldCH);
         output.writeObject(uch.newCH);
      }

      public UnionConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnionConsistentHash((ConsistentHash) input.readObject(), (ConsistentHash) input.readObject());
      }
   }

   public ConsistentHash getOldCH() {
      return oldCH;
   }

   public ConsistentHash getNewCH() {
      return newCH;
   }
}
