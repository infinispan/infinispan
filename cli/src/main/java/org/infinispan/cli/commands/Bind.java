package org.infinispan.cli.commands;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.aesh.readline.action.Action;
import org.aesh.readline.editing.EditMode;
import org.aesh.readline.editing.EditModeBuilder;
import org.aesh.terminal.Key;
import org.aesh.terminal.KeyAction;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @since 16.2
 */
@MetaInfServices(Command.class)
@CommandDefinition(name = "bind", description = "Shows available key bindings")
public class Bind extends CliCommand {

   private static final Set<String> IGNORED_ACTIONS = Set.of("no-action", "self-insert");

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Option(name = "mode", shortName = 'm', description = "Editing mode: emacs (default) or vi", defaultValue = "emacs")
   String mode;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      EditMode.Mode editModeType;
      try {
         editModeType = EditMode.Mode.valueOf(mode.toUpperCase());
      } catch (IllegalArgumentException e) {
         invocation.errorln("Invalid mode: " + mode + ". Use 'emacs' or 'vi'.");
         return CommandResult.FAILURE;
      }

      EditMode editMode = EditModeBuilder.builder(editModeType).create();
      KeyAction[] keys = editMode.keys();

      Map<String, String> bindings = new LinkedHashMap<>();
      for (KeyAction keyAction : keys) {
         Action action = editMode.parse(keyAction);
         editMode.setStatus(EditMode.Status.EDIT);
         if (action == null) {
            continue;
         }
         String actionName = action.name();
         if (actionName == null || IGNORED_ACTIONS.contains(actionName)) {
            continue;
         }
         String keyName = formatKeyAction(keyAction);
         if (keyName != null && !bindings.containsKey(keyName)) {
            bindings.put(keyName, actionName);
         }
      }

      int maxKeyLen = bindings.keySet().stream().mapToInt(String::length).max().orElse(10);
      int maxActLen = bindings.values().stream().mapToInt(String::length).max().orElse(20);

      invocation.printf("Key bindings (%s mode):%n", editModeType.name().toLowerCase());
      invocation.println("-".repeat(maxKeyLen + maxActLen + 4));
      String format = "%-" + maxKeyLen + "s  %s%n";
      bindings.forEach((key, action) -> invocation.printf(format, key, action));

      return CommandResult.SUCCESS;
   }

   private static String formatKeyAction(KeyAction keyAction) {
      if (keyAction instanceof Key key) {
         return formatKeyName(key);
      }
      Key key = Key.getKey(keyAction.buffer().array());
      if (key != null) {
         return formatKeyName(key);
      }
      return null;
   }

   private static String formatKeyName(Key key) {
      String name = key.name();
      if (name.startsWith("CTRL_")) {
         String suffix = name.substring(5);
         return "Ctrl-" + suffix;
      }
      if (name.startsWith("META_")) {
         String suffix = name.substring(5);
         if (suffix.startsWith("CTRL_")) {
            return "Alt-Ctrl-" + suffix.substring(5);
         }
         return "Alt-" + suffix;
      }
      return switch (name) {
         case "UP", "UP_2" -> "Up";
         case "DOWN", "DOWN_2" -> "Down";
         case "LEFT", "LEFT_2" -> "Left";
         case "RIGHT", "RIGHT_2" -> "Right";
         case "HOME", "HOME_2", "HOME_3" -> "Home";
         case "END", "END_2", "END_3" -> "End";
         case "DELETE" -> "Delete";
         case "INSERT" -> "Insert";
         case "BACKSPACE" -> "Backspace";
         case "ENTER", "ENTER_2" -> "Enter";
         case "PGUP", "PGUP_2" -> "PgUp";
         case "PGDOWN", "PGDOWN_2" -> "PgDown";
         case "ESC" -> "Esc";
         case "SPACE" -> "Space";
         case "TAB" -> "Tab";
         default -> null;
      };
   }
}
