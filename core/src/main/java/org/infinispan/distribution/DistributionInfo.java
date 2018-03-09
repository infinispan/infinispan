package org.infinispan.distribution;

import java.util.Collection;
import java.util.List;

import org.infinispan.remoting.transport.Address;

/**
 * @author Radim Vansa
 * @author Dan Berindei
 * @since 9.0
 */
public class DistributionInfo {
   private final int segmentId;
   // The write CH always includes the read CH, and the primary owner is always in the read CH
   private final Address primary;
   private final List<Address> readOwners;
   private final List<Address> writeOwners;
   private final Collection<Address> writeBackups;

   private final boolean isPrimary;
   private final boolean isReadOwner;
   private final boolean isWriteOwner;
   private final boolean isWriteBackup;
   private final boolean anyWriteBackupNonReader;

   public DistributionInfo(int segmentId, Address primary, List<Address> readOwners, List<Address> writeOwners,
                           Collection<Address> writeBackups, Address localAddress) {
      this.segmentId = segmentId;
      this.primary = primary;
      this.readOwners = readOwners;
      this.writeOwners = writeOwners;
      this.writeBackups = writeBackups;

      this.isPrimary = primary != null && primary.equals(localAddress);
      this.isReadOwner = readOwners.contains(localAddress);
      this.isWriteOwner = writeOwners.contains(localAddress);
      this.isWriteBackup = this.isWriteOwner && !this.isPrimary;
      this.anyWriteBackupNonReader = !readOwners.containsAll(writeBackups);
   }


   public int segmentId() {
      return segmentId;
   }

   public Address primary() {
      return primary;
   }

   public List<Address> readOwners() {
      return readOwners;
   }

   public List<Address> writeOwners() {
      return writeOwners;
   }

   public Collection<Address> writeBackups() {
      return writeBackups;
   }

   public boolean isPrimary() {
      return isPrimary;
   }

   public boolean isReadOwner() {
      return isReadOwner;
   }

   public boolean isWriteOwner() {
      return isWriteOwner;
   }

   public boolean isWriteBackup() {
      return isWriteBackup;
   }

   public boolean isAnyWriteBackupNonReader() {
      return anyWriteBackupNonReader;
   }

   public Ownership readOwnership() {
      return isPrimary ? Ownership.PRIMARY : (isReadOwner ? Ownership.BACKUP : Ownership.NON_OWNER);
   }

   public Ownership writeOwnership() {
      return isPrimary ? Ownership.PRIMARY : (isWriteOwner ? Ownership.BACKUP : Ownership.NON_OWNER);
   }
}
