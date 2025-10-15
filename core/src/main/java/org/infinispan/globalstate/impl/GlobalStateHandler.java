package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.ScopedPersistentState.GLOBAL_SCOPE;
import static org.infinispan.globalstate.impl.GlobalStateManagerImpl.TIMESTAMP;
import static org.infinispan.globalstate.impl.GlobalStateManagerImpl.VERSION;
import static org.infinispan.globalstate.impl.GlobalStateManagerImpl.VERSION_MAJOR;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;

public final class GlobalStateHandler implements GlobalStateManager {

   private final List<GlobalStateProvider> providers = new CopyOnWriteArrayList<>();

   private String root;
   private TimeService timeService;

   public GlobalStateHandler(String root, TimeService timeService) {
      this.root = root;
      this.timeService = timeService;
   }

   GlobalStateHandler() { }

   void setRoot(String root) {
      this.root = root;
   }

   void setTimeService(TimeService timeService) {
      this.timeService = timeService;
   }

   // Utilized only by the GlobalStateManagerImpl
   ScopedPersistentState startStateHandler() {
      File stateFile = getStateFile(GLOBAL_SCOPE);
      ScopedPersistentState globalState = readScopedState(GLOBAL_SCOPE).orElse(null);
      if (globalState != null) {
         // We proceed only if we can write to the file
         if (!stateFile.canWrite()) {
            throw CONTAINER.nonWritableStateFile(stateFile);
         }
         // Validate the state before proceeding
         if (!globalState.containsProperty(VERSION) || !globalState.containsProperty(VERSION_MAJOR) || !globalState.containsProperty(TIMESTAMP)) {
            throw CONTAINER.invalidPersistentState(GLOBAL_SCOPE);
         }
         CONTAINER.globalStateLoad(globalState.getProperty(VERSION), globalState.getProperty(TIMESTAMP));
         providers.forEach(provider -> provider.prepareForRestore(globalState));
      } else {
         // Clean slate. Create the persistent location if necessary and acquire a lock
         stateFile.getParentFile().mkdirs();
      }
      return globalState;
   }

   @Override
   public void registerStateProvider(GlobalStateProvider provider) {
      providers.add(provider);
   }

   @Override
   public Optional<ScopedPersistentState> readScopedState(String scope) {
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

   @Override
   public void writeScopedState(ScopedPersistentState state) {
      File stateFile = getStateFile(state.getScope());
      try (PrintWriter w = new PrintWriter(stateFile)) {
         state.forEach((key, value) -> {
            w.printf("%s=%s%n", Util.unicodeEscapeString(key), Util.unicodeEscapeString(value));
         });
      } catch (IOException e) {
         throw CONTAINER.failedWritingGlobalState(e, stateFile);
      }
   }

   @Override
   public void deleteScopedState(String scope) {
      File stateFile = getStateFile(scope);
      try {
         Files.deleteIfExists(stateFile.toPath());
      } catch (IOException e) {
         throw CONTAINER.failedWritingGlobalState(e, stateFile);
      }
   }

   @Override
   public void writeGlobalState() {
      if (providers.isEmpty()) {
         // If no state providers were registered, we cannot persist
         CONTAINER.incompleteGlobalState();
      } else {
         ScopedPersistentState state = new ScopedPersistentStateImpl(GLOBAL_SCOPE);
         state.setProperty(VERSION, Version.getVersion());
         state.setProperty(VERSION_MAJOR, Version.getMajor());
         state.setProperty(TIMESTAMP, timeService.instant().toString());
         // ask any state providers to contribute to the global state
         for (GlobalStateProvider provider : providers) {
            provider.prepareForPersist(state);
         }
         writeScopedState(state);
         CONTAINER.globalStateWrite(state.getProperty(VERSION), state.getProperty(TIMESTAMP));
      }
   }

   private File getStateFile(String scope) {
      return new File(root, scope + ".state");
   }
}
