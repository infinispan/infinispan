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
package org.infinispan.client.hotrod.configuration;

import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration {
   private final boolean enabled;
   private final KeyManager[] keyManagers;
   private final TrustManager[] trustManagers;
   private final String keyStoreFileName;
   private final char[] keyStorePassword;
   private final String trustStoreFileName;
   private final char[] trustStorePassword;

   SslConfiguration(boolean enabled, KeyManager[] keyManagers, String keyStoreFileName, char[] keyStorePassword, TrustManager[] trustManagers, String trustStoreFileName,
         char[] trustStorePassword) {
      this.enabled = enabled;
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
      return "SSLConfiguration [enabled=" + enabled + ", keyManagers=" + Arrays.toString(keyManagers) + ", trustManagers=" + Arrays.toString(trustManagers) + ", keyStoreFileName="
            + keyStoreFileName + ", trustStoreFileName=" + trustStoreFileName + "]";
   }
}
