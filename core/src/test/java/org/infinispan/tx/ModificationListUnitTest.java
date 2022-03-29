package org.infinispan.tx;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.impl.ModificationList;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ModificationList}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "tx.ModificationListUnitTest")
public class ModificationListUnitTest extends AbstractInfinispanTest {

   private static WriteCommand mockCommand(boolean cacheModeLocalFlag) {
      WriteCommand cmd = Mockito.mock(WriteCommand.class);
      Mockito.when(cmd.hasAnyFlag(ArgumentMatchers.anyLong())).thenReturn(cacheModeLocalFlag);
      return cmd;
   }

   public void testZeroAndNegativeCapacity() {
      expectException(IllegalArgumentException.class, () -> new ModificationList(0));
      expectException(IllegalArgumentException.class, () -> new ModificationList(-1));
   }

   public void testGrow() {
      ModificationList modsList = new ModificationList(1);
      int expectedSize = 5;
      for (int i = 0; i < expectedSize; ++i) {
         modsList.append(mockCommand(true));
      }
      assertEquals(expectedSize, modsList.size());
      assertEquals(expectedSize, modsList.getAllModifications().size());
      assertEquals(0, modsList.getModifications().size());

      for (int i = 0; i < expectedSize; ++i) {
         modsList.append(mockCommand(false));
      }
      assertEquals(expectedSize * 2, modsList.size());
      assertEquals(expectedSize * 2, modsList.getAllModifications().size());
      assertEquals(expectedSize, modsList.getModifications().size());
   }

   public void testFreeze() {
      List<WriteCommand> commands = Arrays.asList(mockCommand(false), mockCommand(false), mockCommand(false));
      ModificationList modsList = ModificationList.fromCollection(commands);
      assertEquals(commands.size(), modsList.size());
      assertEquals(commands.size(), modsList.getModifications().size());
      assertEquals(commands.size(), modsList.getAllModifications().size());

      modsList.freeze();

      expectException(IllegalStateException.class, () -> modsList.append(mockCommand(false)));
      expectException(IllegalStateException.class, () -> modsList.append(mockCommand(true)));
   }

   public void testModsOrder() {
      List<WriteCommand> commands = Arrays.asList(mockCommand(false), mockCommand(true), mockCommand(false));

      ModificationList modsList = ModificationList.fromCollection(commands);
      assertEquals(commands.size(), modsList.size());
      assertFalse(modsList.isEmpty());
      assertEquals(2, modsList.getModifications().size());
      assertEquals(3, modsList.getAllModifications().size());
      assertTrue(modsList.hasNonLocalModifications());

      assertEquals(commands, modsList.getAllModifications());
      assertEquals(Arrays.asList(commands.get(0), commands.get(2)), modsList.getModifications());
   }

   public void testSnapshot() {
      List<WriteCommand> commands = Arrays.asList(mockCommand(false), mockCommand(true), mockCommand(false));

      ModificationList modsList = ModificationList.fromCollection(commands);

      List<WriteCommand> allCommands = modsList.getAllModifications();
      List<WriteCommand> nonLocalCommands = modsList.getModifications();

      assertEquals(commands.size(), modsList.size());
      assertTrue(modsList.hasNonLocalModifications());

      assertEquals(2, nonLocalCommands.size());
      assertEquals(3, allCommands.size());
      assertEquals(commands, allCommands);
      assertEquals(Arrays.asList(commands.get(0), commands.get(2)), nonLocalCommands);

      modsList.append(mockCommand(false));
      assertEquals(commands.size() + 1, modsList.size());

      // Snapshot should be unchanged
      assertEquals(2, nonLocalCommands.size());
      assertEquals(3, allCommands.size());
      assertEquals(commands, allCommands);
      assertEquals(Arrays.asList(commands.get(0), commands.get(2)), nonLocalCommands);

      modsList.append(mockCommand(true));
      assertEquals(commands.size() + 2, modsList.size());

      // Snapshot should be unchanged
      assertEquals(2, nonLocalCommands.size());
      assertEquals(3, allCommands.size());
      assertEquals(commands, allCommands);
      assertEquals(Arrays.asList(commands.get(0), commands.get(2)), nonLocalCommands);
   }

}
