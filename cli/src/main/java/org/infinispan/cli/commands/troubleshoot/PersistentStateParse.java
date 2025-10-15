package org.infinispan.cli.commands.troubleshoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.PersistentScopeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.util.concurrent.FileSystemLock;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.GlobalStateHandler;

/**
 * Visualize and manipulate the persistent state.
 *
 * <p>
 * <b>DISCLAIMER:</b> This command is dangerous and should be utilized only for investigation. Deleting a scope is harmful.
 * </p>
 *
 * <p>
 * This command allows to list, read, and delete a scope in the global state. A scope is written during the graceful
 * shutdown procedure and is utilized during start-up to reconstruct the previous cluster and avoid unnecessary state
 * transfer.
 * </p>
 *
 * <p>
 * The operations are:
 * <ul>
 *    <li>Default: This command list all existing scopes in the file system. Usually, there is a scope per cache.</li>
 *    <li><b>show [name]</b>: Shows the data stored in the given scope.</li>
 *    <li><b>delete [name]</b>: Deletes the data stored in the given scope. This operation is dangerous.</li>
 * </ul>
 *
 * All commands accept the path to directory utilized in the server's global-state configuration.
 * </p>
 *
 * @author José Bolina
 * @since 16.0
 */
@CommandDefinition(name = "persistent-state", description = "Inspect the persistent state")
public class PersistentStateParse extends CliCommand implements PersistentScopeCompleter.PersistentScopeAwareCommand {

   @Option(name = "show", completer = PersistentScopeCompleter.class, description = "Show the persistent state of a single scope")
   String show;

   @Option(name = "delete", completer = PersistentScopeCompleter.class, description = "Delete the persistent state of a single scope")
   String delete;

   @Argument(completer = FileOptionCompleter.class, description = "Path to the persistent state directory")
   String path;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   protected boolean isHelp() {
      return help || path == null;
   }

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      if (path == null) return CommandResult.FAILURE;

      Path p = Path.of(path);
      if (Files.notExists(p) || !Files.isDirectory(p)) {
         invocation.getShellError().println("The path " + path + " does not exist or is not a directory");
         return CommandResult.FAILURE;
      }

      if (show != null) {
         showScope(invocation, show);
         return CommandResult.SUCCESS;
      }

      if (delete != null) {
         FileSystemLock lock = new FileSystemLock(p, ScopedPersistentState.GLOBAL_SCOPE);
         try {
            if (!lock.tryLock() && !lock.isAcquired()) {
               invocation.getShellError().printf("Global file lock is held by server at %s. Can not manipulate scoped data%n", path);
               return CommandResult.FAILURE;
            }
            return modifyScope(invocation);
         } catch (IOException e) {
            throw new RuntimeException(e);
         } finally {
            if (lock.isAcquired()) {
               lock.unlock();
            }
         }
      }

      listAllScopes(invocation);
      return CommandResult.SUCCESS;
   }

   private CommandResult modifyScope(ContextAwareCommandInvocation invocation) {
      if (delete != null) {
         showScope(invocation, delete);
         deleteScope(invocation, delete);
      }

      return CommandResult.SUCCESS;
   }

   @Override
   public Collection<String> getPersistentScopes() {
      try (Stream<Path> paths = Files.walk(Path.of(path))) {
         return paths.filter(Files::isRegularFile)
               .filter(p -> p.getFileName().toString().endsWith(".state"))
               .map(p -> p.getFileName().toString().replace(".state", ""))
               .filter(s -> !Objects.equals(s, ScopedPersistentState.GLOBAL_SCOPE))
               .toList();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private void showScope(ContextAwareCommandInvocation invocation, String scope) {
      GlobalStateManager handler = new GlobalStateHandler(path, DefaultTimeService.INSTANCE);
      Optional<ScopedPersistentState> optional = handler.readScopedState(scope);
      if (optional.isEmpty()) {
         invocation.getShellError().println("The scope " + scope + " does not exist in " + path);
         return;
      }

      ScopedPersistentState state = optional.get();
      invocation.getShell().writeln(String.format("Scope=%s; Checksum=%d", scope, state.getChecksum()));
      state.forEach((k, v) -> invocation.getShell().writeln(String.format("key=%s; value=%s", k, v)));
   }

   private void deleteScope(ContextAwareCommandInvocation invocation, String scope) {
      GlobalStateManager handler = new GlobalStateHandler(path, DefaultTimeService.INSTANCE);
      handler.deleteScopedState(scope);
      invocation.getShellOutput().printf("Deleted scope %s in %s.%n", scope, path);
   }

   private void listAllScopes(ContextAwareCommandInvocation invocation) {
      invocation.getShellOutput().printf("List all states in: %s%n", path);
      getPersistentScopes().forEach(invocation.getShell()::writeln);
   }
}
