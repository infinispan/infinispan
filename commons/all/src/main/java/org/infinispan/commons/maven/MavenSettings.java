/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infinispan.commons.maven;

import static org.infinispan.commons.configuration.io.xml.XmlPullParser.END_DOCUMENT;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.END_TAG;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.FEATURE_PROCESS_NAMESPACES;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.START_TAG;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.infinispan.commons.configuration.io.xml.MXParser;
import org.infinispan.commons.configuration.io.xml.XmlPullParser;
import org.infinispan.commons.configuration.io.xml.XmlPullParserException;

/**
 * @author Tomaz Cerar (c) 2014 Red Hat Inc.
 */
public final class MavenSettings {
   private static MavenSettings mavenSettings;
   private Path settingsPath;
   private Path localRepository = null;
   private final List<String> remoteRepositories = new LinkedList<>();
   private final Map<String, Profile> profiles = new HashMap<>();
   private final List<String> activeProfileNames = new LinkedList<>();
   private final List<Proxy> proxies = new ArrayList<>();

   MavenSettings() {
      configureDefaults();
   }

   public static MavenSettings init() throws IOException {
      return init(null);
   }

   public static MavenSettings init(Path settingsPath) throws IOException {
      if (mavenSettings != null && Objects.equals(mavenSettings.settingsPath, settingsPath)) {
         return mavenSettings;
      }
      mavenSettings = new MavenSettings();
      Path m2 = Paths.get(System.getProperty("user.home"), ".m2");

      if (settingsPath == null) {
         settingsPath = m2.resolve("settings.xml");
      }

      if (Files.notExists(settingsPath)) {
         String mavenHome = System.getenv("M2_HOME");
         if (mavenHome != null) {
            settingsPath = Paths.get(mavenHome, "conf", "settings.xml");
         }
      }
      mavenSettings.settingsPath = settingsPath;
      if (Files.exists(settingsPath)) {
         parseSettingsXml(settingsPath, mavenSettings);
      }
      if (mavenSettings.getLocalRepository() == null) {
         Path repository = m2.resolve("repository");
         mavenSettings.setLocalRepository(repository);
      }
      mavenSettings.resolveActiveSettings();
      return mavenSettings;
   }

   static MavenSettings parseSettingsXml(Path settings, MavenSettings mavenSettings) throws IOException {
      try {
         final MXParser reader = new MXParser();
         reader.setFeature(FEATURE_PROCESS_NAMESPACES, false);
         InputStream source = Files.newInputStream(settings, StandardOpenOption.READ);
         reader.setInput(source, null);
         int eventType;
         while ((eventType = reader.next()) != END_DOCUMENT) {
            switch (eventType) {
               case START_TAG: {
                  switch (reader.getName()) {
                     case "settings": {
                        parseSettings(reader, mavenSettings);
                        break;
                     }
                  }
               }
               default: {
                  break;
               }
            }
         }
         return mavenSettings;
      } catch (XmlPullParserException e) {
         throw new IOException("Could not parse maven settings.xml");
      }
   }

   static void parseSettings(final XmlPullParser reader, MavenSettings mavenSettings) throws XmlPullParserException, IOException {
      int eventType;
      while ((eventType = reader.nextTag()) != END_DOCUMENT) {
         switch (eventType) {
            case END_TAG: {
               return;
            }
            case START_TAG: {
               switch (reader.getName()) {
                  case "localRepository": {
                     String localRepository = reader.nextText();
                     boolean localRepositoryNotDefinedViaProperty = mavenSettings.getLocalRepository() == null;
                     if (localRepositoryNotDefinedViaProperty && localRepository != null && !localRepository.trim().isEmpty()) {
                        mavenSettings.setLocalRepository(Paths.get(interpolateVariables(localRepository)));
                     }
                     break;
                  }
                  case "proxies": {
                     while ((eventType = reader.nextTag()) != END_DOCUMENT) {
                        if (eventType == START_TAG) {
                           switch (reader.getName()) {
                              case "proxy": {
                                 parseProxy(reader, mavenSettings);
                                 break;
                              }
                           }
                        } else {
                           break;
                        }
                     }
                     break;
                  }
                  case "profiles": {
                     while ((eventType = reader.nextTag()) != END_DOCUMENT) {
                        if (eventType == START_TAG) {
                           switch (reader.getName()) {
                              case "profile": {
                                 parseProfile(reader, mavenSettings);
                                 break;
                              }
                           }
                        } else {
                           break;
                        }
                     }
                     break;
                  }
                  case "activeProfiles": {
                     while ((eventType = reader.nextTag()) != END_DOCUMENT) {
                        if (eventType == START_TAG) {
                           switch (reader.getName()) {
                              case "activeProfile": {
                                 mavenSettings.addActiveProfile(reader.nextText());
                                 break;
                              }
                           }
                        } else {
                           break;
                        }

                     }
                     break;
                  }
                  default: {
                     skip(reader);

                  }
               }
               break;
            }
            default: {
               throw new IOException("Unexpected content");
            }
         }
      }
      throw new IOException("Unexpected end of document");
   }

   static void parseProxy(final XmlPullParser reader, MavenSettings mavenSettings) throws XmlPullParserException, IOException {
      int eventType;
      Proxy proxy = new Proxy();
      while ((eventType = reader.nextTag()) != END_DOCUMENT) {
         if (eventType == START_TAG) {
            switch (reader.getName()) {
               case "id": {
                  proxy.setId(reader.nextText());
                  break;
               }
               case "active": {
                  proxy.setActive(Boolean.parseBoolean(reader.nextText()));
                  break;
               }
               case "protocol": {
                  proxy.setProtocol(reader.nextText());
                  break;
               }
               case "host": {
                  proxy.setHost(reader.nextText());
                  break;
               }
               case "port": {
                  proxy.setPort(Integer.parseInt(reader.nextText()));
                  break;
               }
               case "username": {
                  proxy.setUsername(reader.nextText());
                  break;
               }
               case "password": {
                  proxy.setPassword(reader.nextText());
                  break;
               }
               case "nonProxyHosts": {
                  proxy.setNonProxyHosts(reader.nextText());
                  break;
               }
               default: {
                  skip(reader);
               }
            }
         } else {
            break;
         }
      }
      mavenSettings.addProxy(proxy);
   }

   static void parseProfile(final XmlPullParser reader, MavenSettings mavenSettings) throws XmlPullParserException, IOException {
      int eventType;
      Profile profile = new Profile();
      while ((eventType = reader.nextTag()) != END_DOCUMENT) {
         if (eventType == START_TAG) {
            switch (reader.getName()) {
               case "id": {
                  profile.setId(reader.nextText());
                  break;
               }
               case "repositories": {
                  while ((eventType = reader.nextTag()) != END_DOCUMENT) {
                     if (eventType == START_TAG) {
                        switch (reader.getName()) {
                           case "repository": {
                              parseRepository(reader, profile);
                              break;
                           }
                        }
                     } else {
                        break;
                     }

                  }
                  break;
               }
               default: {
                  skip(reader);
               }
            }
         } else {
            break;
         }
      }
      mavenSettings.addProfile(profile);
   }

   static void parseRepository(final XmlPullParser reader, Profile profile) throws XmlPullParserException, IOException {
      int eventType;
      while ((eventType = reader.nextTag()) != END_DOCUMENT) {
         if (eventType == START_TAG) {
            switch (reader.getName()) {
               case "url": {
                  profile.addRepository(reader.nextText());
                  break;
               }
               default: {
                  skip(reader);
               }
            }
         } else {
            break;
         }

      }
   }

   static void skip(XmlPullParser parser) throws XmlPullParserException, IOException {
      if (parser.getEventType() != XmlPullParser.START_TAG) {
         throw new IllegalStateException();
      }
      int depth = 1;
      while (depth != 0) {
         switch (parser.next()) {
            case XmlPullParser.END_TAG:
               depth--;
               break;
            case XmlPullParser.START_TAG:
               depth++;
               break;
         }
      }
   }

   void configureDefaults() {
      //always add maven central
      remoteRepositories.add("https://repo1.maven.org/maven2/");
      String localRepositoryPath = System.getProperty("local.maven.repo.path");
      if (localRepositoryPath != null && !localRepositoryPath.trim().isEmpty()) {
         System.out.println("Please use 'maven.repo.local' instead of 'local.maven.repo.path'");
         localRepository = Paths.get(localRepositoryPath.split(File.pathSeparator)[0]);
      }

      localRepositoryPath = System.getProperty("maven.repo.local");
      if (localRepositoryPath != null && !localRepositoryPath.trim().isEmpty()) {
         localRepository = Paths.get(localRepositoryPath);
      }
      String remoteRepository = System.getProperty("remote.maven.repo");
      if (remoteRepository != null) {
         for (String repo : remoteRepository.split(",")) {
            if (!repo.endsWith("/")) {
               repo += "/";
            }
            remoteRepositories.add(repo);
         }
      }
   }

   public void setLocalRepository(Path localRepository) {
      this.localRepository = localRepository;
   }

   public Path getLocalRepository() {
      return localRepository;
   }

   public List<String> getRemoteRepositories() {
      return remoteRepositories;
   }

   public void addProfile(Profile profile) {
      this.profiles.put(profile.getId(), profile);
   }

   public void addActiveProfile(String profileName) {
      activeProfileNames.add(profileName);
   }

   public void addProxy(Proxy proxy) {
      this.proxies.add(proxy);
   }

   public List<Proxy> getProxies() {
      return this.proxies;
   }

   public Proxy getProxyFor(URL url) {
      for (Proxy proxy : this.proxies) {
         if (proxy.canProxyFor(url)) {
            return proxy;
         }
      }

      return null;
   }

   /**
    * Opens a connection with appropriate proxy and credentials, if required.
    *
    * @param url The URL to open.
    * @return The opened connection.
    * @throws IOException If an error occurs establishing the connection.
    */
   public URLConnection openConnection(URL url) throws IOException {
      Proxy proxy = getProxyFor(url);
      URLConnection conn = null;

      if (proxy != null) {
         conn = url.openConnection(proxy.getProxy());
         proxy.authenticate(conn);
      } else {
         conn = url.openConnection();
      }

      return conn;
   }

   void resolveActiveSettings() {
      for (String name : activeProfileNames) {
         Profile p = profiles.get(name);
         if (p != null) {
            remoteRepositories.addAll(p.getRepositories());
         }
      }
   }

   static String interpolateVariables(String in) {
      StringBuilder out = new StringBuilder();

      int cur = 0;
      int startLoc = -1;

      while ((startLoc = in.indexOf("${", cur)) >= 0) {
         out.append(in.substring(cur, startLoc));
         int endLoc = in.indexOf("}", startLoc);
         if (endLoc > 0) {
            String name = in.substring(startLoc + 2, endLoc);
            String value = null;
            if (name.startsWith("env.")) {
               value = System.getenv(name.substring(4));
            } else {
               value = System.getProperty(name);
            }
            if (value == null) {
               value = "";
            }
            out.append(value);
         } else {
            out.append(in.substring(startLoc));
            cur = in.length();
            break;
         }
         cur = endLoc + 1;
      }

      out.append(in.substring(cur));

      return out.toString();
   }

   static final class Proxy {
      private String id;

      private boolean active = true;

      private String protocol = "http";

      private String host;

      private int port;

      private String username;

      private String password;

      private Set<NonProxyHost> nonProxyHosts = new HashSet<>();

      private AtomicReference<java.net.Proxy> netProxy = new AtomicReference<>();

      public String getId() {
         return id;
      }

      public void setId(String id) {
         this.id = id;
      }

      public boolean isActive() {
         return active;
      }

      public void setActive(boolean active) {
         this.active = active;
      }

      public String getProtocol() {
         return protocol;
      }

      public void setProtocol(String protocol) {
         this.protocol = protocol;
      }

      public String getHost() {
         return host;
      }

      public void setHost(String host) {
         this.host = host;
      }

      public int getPort() {
         return port;
      }

      public void setPort(int port) {
         this.port = port;
      }

      public String getUsername() {
         return username;
      }

      public void setUsername(String username) {
         this.username = username;
      }

      public String getPassword() {
         return password;
      }

      public void setPassword(String password) {
         this.password = password;
      }

      public void setNonProxyHosts(String nonProxyHosts) {
         String[] specs = nonProxyHosts.split("\\|");
         this.nonProxyHosts.clear();
         for (String spec : specs) {
            this.nonProxyHosts.add(new NonProxyHost(spec));
         }
      }

      public boolean canProxyFor(URL url) {
         for (NonProxyHost nonProxyHost : this.nonProxyHosts) {
            if (nonProxyHost.matches(url)) {
               return false;
            }
         }

         return true;
      }

      public java.net.Proxy getProxy() {
         return this.netProxy.updateAndGet(proxy -> {
            if (proxy == null) {
               proxy = new java.net.Proxy(java.net.Proxy.Type.HTTP,
                     new InetSocketAddress(getHost(), getPort()));
            }

            return proxy;
         });
      }

      public String getCredentialsBase64() {
         Base64.Encoder encoder = Base64.getEncoder();
         return encoder.encodeToString((this.username + ":" + this.password).getBytes());
      }

      public void authenticate(URLConnection conn) {
         if (this.username == null && this.password == null) {
            return;
         }
         String authz = "Basic " + getCredentialsBase64();
         conn.addRequestProperty("Proxy-Authorization", authz);
      }
   }

   static final class NonProxyHost {
      private final Pattern pattern;

      NonProxyHost(String spec) {

         spec = spec.replace(".", "\\.");
         spec = spec.replace("*", ".*");
         spec = "^" + spec + "$";
         this.pattern = Pattern.compile(spec);
      }

      boolean matches(URL url) {
         return this.pattern.matcher(url.getHost()).matches();
      }
   }

   static final class Profile {
      private String id;

      final List<String> repositories = new LinkedList<>();

      Profile() {

      }

      public void setId(String id) {
         this.id = id;
      }

      public String getId() {
         return id;
      }

      public void addRepository(String url) {
         if (!url.endsWith("/")) {
            url += "/";
         }
         repositories.add(url);
      }

      public List<String> getRepositories() {
         return repositories;
      }
   }
}
