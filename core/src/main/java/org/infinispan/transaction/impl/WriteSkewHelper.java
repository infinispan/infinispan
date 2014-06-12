package org.infinispan.transaction.impl;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.TimeService;

/**
 * Encapsulates write skew logic in maintaining version maps, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewHelper {

   public static void readVersionsFromResponse(Response r, CacheTransaction ct) {
      if (r != null && r.isSuccessful()) {
         SuccessfulResponse sr = (SuccessfulResponse) r;
         EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
         if (uv != null) ct.setUpdatedEntryVersions(uv.merge(ct.getUpdatedEntryVersions()));
      }
   }

   public static EntryVersionsMap performWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                            DataContainer dataContainer,
                                                                            PersistenceManager persistenceManager,
                                                                            VersionGenerator versionGenerator,
                                                                            TxInvocationContext context,
                                                                            KeySpecificLogic ksl, TimeService timeService) {
      EntryVersionsMap uv = new EntryVersionsMap();
      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            if (ksl.performCheckOnKey(k)) {
               ClusteredRepeatableReadEntry entry = (ClusteredRepeatableReadEntry) context.lookupEntry(k);

               if (entry.performWriteSkewCheck(dataContainer, persistenceManager, context,
                                               prepareCommand.getVersionsSeen().get(k), versionGenerator, timeService)) {
                  IncrementableEntryVersion newVersion = entry.isCreated()
                        ? versionGenerator.generateNew()
                        : versionGenerator.increment((IncrementableEntryVersion) entry.getMetadata().version());
                  uv.put(k, newVersion);
               } else {
                  // Write skew check detected!
                  throw new WriteSkewException("Write skew detected on key " + k + " for transaction " +
                                                     context.getTransaction(), k);
               }
            }
         }
      }
      return uv;
   }

   public static EntryVersionsMap performTotalOrderWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                                      DataContainer dataContainer,
                                                                                      PersistenceManager persistenceManager,
                                                                                      VersionGenerator versionGenerator,
                                                                                      TxInvocationContext context,
                                                                                      KeySpecificLogic ksl,
                                                                                      TimeService timeService) {
      EntryVersionsMap uv = new EntryVersionsMap();
      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            if (ksl.performCheckOnKey(k)) {
               ClusteredRepeatableReadEntry entry = (ClusteredRepeatableReadEntry) context.lookupEntry(k);

               if (entry.performWriteSkewCheck(dataContainer, persistenceManager, context,
                                               prepareCommand.getVersionsSeen().get(k), versionGenerator, timeService)) {
                  //in total order, it does not care about the version returned. It just need the keys validated
                  uv.put(k, null);
               } else {
                  // Write skew check detected!
                  throw new WriteSkewException("Write skew detected on key " + k + " for transaction " +
                                                     context.getTransaction(), k);
               }
            }
         }
      }
      return uv;
   }
   
   public static interface KeySpecificLogic {
      boolean performCheckOnKey(Object key);
   }
}
