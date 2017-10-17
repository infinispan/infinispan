package org.infinispan.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.triangle.MultiEntriesFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.MultiKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.PutMapBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.remoting.responses.ValidResponse;

/**
 * Some utility functions for {@link org.infinispan.interceptors.distribution.TriangleDistributionInterceptor}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class TriangleFunctionsUtil {

   private TriangleFunctionsUtil() {
   }

   public static PutMapCommand copy(PutMapCommand command, Collection<Object> keys) {
      PutMapCommand copy = new PutMapCommand(command, false);
      copy.setMap(filterEntries(command.getMap(), keys));
      return copy;
   }

   public static <K, V, T> WriteOnlyManyEntriesCommand<K, V, T> copy(WriteOnlyManyEntriesCommand<K, V, T> command,
         Collection<Object> keys) {
      return new WriteOnlyManyEntriesCommand<>(command).withArguments(filterEntries(command.getArguments(), keys));
   }

   public static <K, V> WriteOnlyManyCommand<K, V> copy(WriteOnlyManyCommand<K, V> command, Collection<Object> keys) {
      WriteOnlyManyCommand<K, V> copy = new WriteOnlyManyCommand<>(command);
      copy.setKeys(keys);
      return copy;
   }

   public static <K, V, R> ReadWriteManyCommand<K, V, R> copy(ReadWriteManyCommand<K, V, R> command,
         Collection<Object> keys) {
      ReadWriteManyCommand<K, V, R> copy = new ReadWriteManyCommand<>(command);
      copy.setKeys(keys);
      return copy;
   }

   public static <K, V, T, R> ReadWriteManyEntriesCommand<K, V, T, R> copy(ReadWriteManyEntriesCommand<K, V, T, R> command,
         Collection<Object> keys) {
      return new ReadWriteManyEntriesCommand<K, V, T, R>(command).withArguments(filterEntries(command.getArguments(), keys));
   }

   public static Map<Object, Object> mergeHashMap(ValidResponse response, Map<Object, Object> resultMap) {
      //noinspection unchecked
      Map<Object, Object> remoteMap = (Map<Object, Object>) response.getResponseValue();
      return InfinispanCollections.mergeMaps(resultMap, remoteMap);
   }

   @SuppressWarnings("unused")
   public static Void voidMerge(ValidResponse ignored1, Void ignored2) {
      return null;
   }

   public static List<Object> mergeList(ValidResponse response, List<Object> resultList) {
      //noinspection unchecked
      List<Object> list = (List<Object>) response.getResponseValue();
      return InfinispanCollections.mergeLists(list, resultList);
   }

   public static Map<Integer, Collection<Object>> filterBySegment(LocalizedCacheTopology cacheTopology,
         Collection<Object> keys) {
      Map<Integer, Collection<Object>> filteredKeys = new HashMap<>(
            cacheTopology.getReadConsistentHash().getNumSegments());
      for (Object key : keys) {
         filteredKeys.computeIfAbsent(cacheTopology.getSegment(key), integer -> new ArrayList<>()).add(key);
      }
      return filteredKeys;
   }


   public static <K, V> Map<K, V> filterEntries(Map<K, V> map, Collection<Object> keys) {
      //note: can't use Collector.toMap() since the implementation doesn't support null values.
      return map.entrySet().stream()
            .filter(entry -> keys.contains(entry.getKey()))
            .collect(HashMap::new, (rMap, entry) -> rMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, PutKeyValueCommand command) {
      SingleKeyBackupWriteCommand cmd = factory.buildSingleKeyBackupWriteCommand();
      cmd.setPutKeyValueCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, RemoveCommand command) {
      SingleKeyBackupWriteCommand cmd = factory.buildSingleKeyBackupWriteCommand();
      cmd.setRemoveCommand(command, command.getCommandId() == RemoveExpiredCommand.COMMAND_ID);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, ReplaceCommand command) {
      SingleKeyBackupWriteCommand cmd = factory.buildSingleKeyBackupWriteCommand();
      cmd.setReplaceCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, ComputeIfAbsentCommand command) {
      SingleKeyBackupWriteCommand cmd = factory.buildSingleKeyBackupWriteCommand();
      cmd.setComputeIfAbsentCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, ComputeCommand command) {
      SingleKeyBackupWriteCommand cmd = factory.buildSingleKeyBackupWriteCommand();
      cmd.setComputeCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, ReadWriteKeyValueCommand command) {
      SingleKeyFunctionalBackupWriteCommand cmd = factory.buildSingleKeyFunctionalBackupWriteCommand();
      cmd.setReadWriteKeyValueCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, ReadWriteKeyCommand command) {
      SingleKeyFunctionalBackupWriteCommand cmd = factory.buildSingleKeyFunctionalBackupWriteCommand();
      cmd.setReadWriteKeyCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, WriteOnlyKeyValueCommand command) {
      SingleKeyFunctionalBackupWriteCommand cmd = factory.buildSingleKeyFunctionalBackupWriteCommand();
      cmd.setWriteOnlyKeyValueCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, WriteOnlyKeyCommand command) {
      SingleKeyFunctionalBackupWriteCommand cmd = factory.buildSingleKeyFunctionalBackupWriteCommand();
      cmd.setWriteOnlyKeyCommand(command);
      return cmd;
   }

   public static BackupWriteCommand backupFrom(CommandsFactory factory, PutMapCommand command,
         Collection<Object> keys) {
      PutMapBackupWriteCommand cmd = factory.buildPutMapBackupWriteCommand();
      cmd.setPutMapCommand(command, keys);
      return cmd;
   }

   public static <K, V, T> BackupWriteCommand backupFrom(CommandsFactory factory,
         WriteOnlyManyEntriesCommand<K, V, T> command, Collection<Object> keys) {
      MultiEntriesFunctionalBackupWriteCommand cmd = factory.buildMultiEntriesFunctionalBackupWriteCommand();
      cmd.setWriteOnly(command, keys);
      return cmd;
   }

   public static <K, V, T, R> BackupWriteCommand backupFrom(CommandsFactory factory,
         ReadWriteManyEntriesCommand<K, V, T, R> command, Collection<Object> keys) {
      MultiEntriesFunctionalBackupWriteCommand cmd = factory.buildMultiEntriesFunctionalBackupWriteCommand();
      cmd.setReadWrite(command, keys);
      return cmd;
   }

   public static <K, V> BackupWriteCommand backupFrom(CommandsFactory factory,
         WriteOnlyManyCommand<K, V> command, Collection<Object> keys) {
      MultiKeyFunctionalBackupWriteCommand cmd = factory.buildMultiKeyFunctionalBackupWriteCommand();
      cmd.setWriteOnly(command, keys);
      return cmd;
   }

   public static <K, V, R> BackupWriteCommand backupFrom(CommandsFactory factory,
         ReadWriteManyCommand<K, V, R> command, Collection<Object> keys) {
      MultiKeyFunctionalBackupWriteCommand cmd = factory.buildMultiKeyFunctionalBackupWriteCommand();
      cmd.setReadWrite(command, keys);
      return cmd;
   }
}
