/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.core.configuration;

import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

public class SslConfiguration {
   private final boolean enabled;
   private final boolean needClientAuth;
   private final KeyManager[] keyManagers;
   private final TrustManager[] trustManagers;
   private final String keyStoreFileName;
   private final char[] keyStorePassword;
   private final String trustStoreFileName;
   private final char[] trustStorePassword;

   SslConfiguration(boolean enabled, boolean needClientAuth, KeyManager[] keyManagers, String keyStoreFileName, char[] keyStorePassword, TrustManager[] trustManagers, String trustStoreFileName,
         char[] trustStorePassword) {
      this.enabled = enabled;
      this.needClientAuth = needClientAuth;
      this.keyManagers = keyManagers;
      this.keyStoreFileName = keyStoreFileName;
      this.keyStorePassword = keyStorePassword;
      this.trustManagers = trustManagers;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePassword = trustStorePassword;
   }

   public boolean enabled() {
      return enabled;
   }

   public boolean needClientAuth() {
      return needClientAuth;
   }

   public KeyManager[] keyManagers() {
      return keyManagers;
   }

   public TrustManager[] trustManagers() {
      return trustManagers;
   }

   public String keyStoreFileName() {
      return keyStoreFileName;
   }

   public char[] keyStorePassword() {
      return keyStorePassword;
   }

   public String trustStoreFileName() {
      return trustStoreFileName;
   }

   public char[] trustStorePassword() {
      return trustStorePassword;
   }

   @Override
   public String toString() {
      return "SslConfiguration [enabled=" + enabled + ", needClientAuth=" + needClientAuth + ", keyManagers=" + Arrays.toString(keyManagers) + ", trustManagers="
            + Arrays.toString(trustManagers) + ", keyStoreFileName=" + keyStoreFileName + ", trustStoreFileName=" + trustStoreFileName + "]";
   }
}
