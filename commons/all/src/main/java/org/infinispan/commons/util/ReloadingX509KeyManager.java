package org.infinispan.commons.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.logging.Log;

/**
 * A {@link X509ExtendedKeyManager} which uses a @{@link FileWatcher} to check for changes.
 */
public final class ReloadingX509KeyManager extends X509ExtendedKeyManager implements Closeable {
   private final AtomicReference<X509ExtendedKeyManager> manager;
   private final Path path;
   private final Function<Path, X509ExtendedKeyManager> action;
   private final FileWatcher watcher;
   private Instant lastLoaded;

   public ReloadingX509KeyManager(FileWatcher watcher, Path path, Function<Path, X509ExtendedKeyManager> action) {
      Objects.requireNonNull(watcher, "watcher must be non-null");
      Objects.requireNonNull(path, "path must be non-null");
      Objects.requireNonNull(action, "action must be non-null");

      this.manager = new AtomicReference<>();
      this.watcher = watcher;
      this.path = path;
      this.action = action;
      reload(this.path);
      watcher.watch(path, this::reload);
   }

   private void reload(Path path) {
      manager.set(action.apply(path));
      lastLoaded = Instant.now();
      Log.SECURITY.debugf("Loaded '%s'", path);
   }

   @Override
   public String[] getClientAliases(String keyType, Principal[] issuers) {
      return manager.get().getClientAliases(keyType, issuers);
   }

   @Override
   public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
      return manager.get().chooseClientAlias(keyType, issuers, socket);
   }

   @Override
   public String[] getServerAliases(String keyType, Principal[] issuers) {
      return manager.get().getServerAliases(keyType, issuers);
   }

   @Override
   public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
      return manager.get().chooseServerAlias(keyType, issuers, socket);
   }

   @Override
   public X509Certificate[] getCertificateChain(String alias) {
      return manager.get().getCertificateChain(alias);
   }

   @Override
   public PrivateKey getPrivateKey(String alias) {
      return manager.get().getPrivateKey(alias);
   }

   @Override
   public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
      return manager.get().chooseEngineClientAlias(keyType, issuers, engine);
   }

   @Override
   public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
      return manager.get().chooseEngineServerAlias(keyType, issuers, engine);
   }

   public Instant lastLoaded() {
      return lastLoaded;
   }

   @Override
   public void close() throws IOException {
      watcher.unwatch(path);
   }
}
