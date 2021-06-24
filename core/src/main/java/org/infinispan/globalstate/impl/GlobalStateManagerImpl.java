package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.ScopedPersistentState.GLOBAL_SCOPE;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;

/**
 * GlobalStateManagerImpl. This global component manages persistent state across restarts as well as global
 * configurations. The information is stored in a Properties file. On a graceful shutdown it persists the following
 * information:
 * <p>
 * version = full version (e.g. major.minor.micro.qualifier) timestamp = timestamp using ISO-8601
 * <p>
 * as well as any additional information contributed by registered {@link GlobalStateProvider}s
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public class GlobalStateManagerImpl implements GlobalStateManager {
   public static final String VERSION = "@version";
   public static final String TIMESTAMP = "@timestamp";
   public static final String VERSION_MAJOR = "version-major";

   @Inject
   GlobalConfiguration globalConfiguration;
   @Inject
   TimeService timeService;

   private List<GlobalStateProvider> stateProviders = new ArrayList<>();
   private boolean persistentState;
   FileOutputStream globalLockFile;
   private FileLock globalLock;
   private ScopedPersistentState globalState;

   @Start(priority = 1) // Must start before everything else
   public void start() {
      persistentState = globalConfiguration.globalState().enabled();
      if (persistentState) {
         acquireGlobalLock();
         loadGlobalState();
      }
   }

   @Stop(priority = 1) // Must write global state before other global components shut down
   public void stop() {
      if (persistentState) {
         writeGlobalState();
         releaseGlobalLock();
      }
   }

   private void acquireGlobalLock() {
      File lockFile = getLockFile();
      try {
         lockFile.getParentFile().mkdirs();
         globalLockFile = new FileOutputStream(lockFile);
         globalLock = globalLockFile.getChannel().tryLock();
         if (globalLock == null) {
            throw CONTAINER.globalStateCannotAcquireLockFile(null, lockFile);
         }
      } catch (IOException | OverlappingFileLockException e) {
         throw CONTAINER.globalStateCannotAcquireLockFile(e, lockFile);
      }
   }

   private void releaseGlobalLock() {
      if (globalLock != null && globalLock.isValid())
         Util.close(globalLock);
      globalLock = null;
      Util.close(globalLockFile);
      getLockFile().delete();
   }

   private void loadGlobalState() {
      File stateFile = getStateFile(GLOBAL_SCOPE);
      globalState = readScopedState(GLOBAL_SCOPE).orElse(null);
      if (globalState != null) {
         ScopedPersistentState state = globalState;
         // We proceed only if we can write to the file
         if (!stateFile.canWrite()) {
            throw CONTAINER.nonWritableStateFile(stateFile);
         }
         // Validate the state before proceeding
         if (!state.containsProperty(VERSION) || !state.containsProperty(VERSION_MAJOR) || !state.containsProperty(TIMESTAMP)) {
            throw CONTAINER.invalidPersistentState(GLOBAL_SCOPE);
         }
         CONTAINER.globalStateLoad(state.getProperty(VERSION), state.getProperty(TIMESTAMP));

         stateProviders.forEach(provider -> provider.prepareForRestore(state));
      } else {
         // Clean slate. Create the persistent location if necessary and acquire a lock
         stateFile.getParentFile().mkdirs();
      }
   }

   @Override
   public void writeGlobalState() {
      if (persistentState) {
         if (stateProviders.isEmpty()) {
            // If no state providers were registered, we cannot persist
            CONTAINER.incompleteGlobalState();
         } else {
            ScopedPersistentState state = new ScopedPersistentStateImpl(GLOBAL_SCOPE);
            state.setProperty(VERSION, Version.getVersion());
            state.setProperty(VERSION_MAJOR, Version.getMajor());
            state.setProperty(TIMESTAMP, timeService.instant().toString());
            // ask any state providers to contribute to the global state
            for (GlobalStateProvider provider : stateProviders) {
               provider.prepareForPersist(state);
            }
            writeScopedState(state);
            CONTAINER.globalStateWrite(state.getProperty(VERSION), state.getProperty(TIMESTAMP));
         }
      }
   }

   @Override
   public void writeScopedState(ScopedPersistentState state) {
      if (persistentState) {
         File stateFile = getStateFile(state.getScope());
         try (PrintWriter w = new PrintWriter(stateFile)) {
            state.forEach((key, value) -> {
               w.printf("%s=%s%n", Util.unicodeEscapeString(key), Util.unicodeEscapeString(value));
            });
         } catch (IOException e) {
            throw CONTAINER.failedWritingGlobalState(e, stateFile);
         }
      }
   }

   @Override
   public void deleteScopedState(String scope) {
      if (persistentState) {
         File stateFile = getStateFile(scope);
         try {
            Files.deleteIfExists(stateFile.toPath());
         } catch (IOException e) {
            throw CONTAINER.failedWritingGlobalState(e, stateFile);
         }
      }
   }

   @Override
   public Optional<ScopedPersistentState> readScopedState(String scope) {
      if (!persistentState)
         return Optional.empty();
      File stateFile = getStateFile(scope);
      if (!stateFile.exists())
         return Optional.empty();
      try (BufferedReader r = new BufferedReader(new FileReader(stateFile))) {
         ScopedPersistentState state = new ScopedPersistentStateImpl(scope);
         for (String line = r.readLine(); line != null; line = r.readLine()) {
            if (!line.startsWith("#")) { // Skip comment lines
               int eq = line.indexOf('=');
               while (eq > 0 && line.charAt(eq - 1) == '\\') {
                  eq = line.indexOf('=', eq + 1);
               }
               if (eq > 0) {
                  state.setProperty(Util.unicodeUnescapeString(line.substring(0, eq).trim()),
                        Util.unicodeUnescapeString(line.substring(eq + 1).trim()));
               }
            }
         }
         return Optional.of(state);
      } catch (IOException e) {
         throw CONTAINER.failedReadingPersistentState(e, stateFile);
      }
   }

   private File getStateFile(String scope) {
      return new File(globalConfiguration.globalState().persistentLocation(), scope + ".state");
   }

   private File getLockFile() {
      return new File(globalConfiguration.globalState().persistentLocation(), GLOBAL_SCOPE + ".lck");
   }

   @Override
   public void registerStateProvider(GlobalStateProvider provider) {
      this.stateProviders.add(provider);
      if (globalState != null) {
         provider.prepareForRestore(globalState);
      }
   }
}
