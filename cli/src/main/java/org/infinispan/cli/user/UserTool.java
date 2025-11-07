package org.infinispan.cli.user;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.util.Util;
import org.wildfly.common.iteration.ByteIterator;
import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.password.Password;
import org.wildfly.security.password.PasswordFactory;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.interfaces.DigestPassword;
import org.wildfly.security.password.interfaces.ScramDigestPassword;
import org.wildfly.security.password.spec.BasicPasswordSpecEncoding;
import org.wildfly.security.password.spec.DigestPasswordAlgorithmSpec;
import org.wildfly.security.password.spec.EncryptablePasswordSpec;
import org.wildfly.security.password.spec.IteratedSaltedPasswordAlgorithmSpec;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class UserTool {
   public static final Supplier<Provider[]> PROVIDERS = () -> new Provider[]{WildFlyElytronPasswordProvider.getInstance()};
   public static final String DEFAULT_USERS_PROPERTIES_FILE = "users.properties";
   public static final String DEFAULT_GROUPS_PROPERTIES_FILE = "groups.properties";
   public static final String DEFAULT_REALM_NAME = "default";
   public static final String DEFAULT_SERVER_ROOT = "server";

   private static final String COMMENT_PREFIX1 = "#";
   private static final String COMMENT_PREFIX2 = "!";
   private static final String REALM_COMMENT_PREFIX = "$REALM_NAME=";
   private static final String COMMENT_SUFFIX = "$";
   private static final String ALGORITHM_COMMENT_PREFIX = "$ALGORITHM=";


   public static final List<String> DEFAULT_ALGORITHMS = List.of(
         ScramDigestPassword.ALGORITHM_SCRAM_SHA_1,
         ScramDigestPassword.ALGORITHM_SCRAM_SHA_256,
         ScramDigestPassword.ALGORITHM_SCRAM_SHA_384,
         ScramDigestPassword.ALGORITHM_SCRAM_SHA_512,
         DigestPassword.ALGORITHM_DIGEST_MD5,
         DigestPassword.ALGORITHM_DIGEST_SHA,
         DigestPassword.ALGORITHM_DIGEST_SHA_256,
         DigestPassword.ALGORITHM_DIGEST_SHA_384,
         DigestPassword.ALGORITHM_DIGEST_SHA_512
   );

   private final Path usersFile;
   private final Path groupsFile;
   private final Properties users = new Properties();
   private final Properties groups = new Properties();
   private String realm = null;
   private Encryption encryption = Encryption.DEFAULT;

   public UserTool(String serverRoot) {
      this(serverRoot, DEFAULT_USERS_PROPERTIES_FILE, DEFAULT_GROUPS_PROPERTIES_FILE);
   }

   public UserTool(String serverRoot, String usersFile, String groupsFile) {
      this(serverRoot != null ? Paths.get(serverRoot) : null,
            usersFile != null ? Paths.get(usersFile) : null,
            groupsFile != null ? Paths.get(groupsFile) : null);
   }

   public UserTool(Path serverRoot, Path usersFile, Path groupsFile) {
      Path serverRoot1;
      if (serverRoot != null && serverRoot.isAbsolute()) {
         serverRoot1 = serverRoot;
      } else {
         String serverHome = System.getProperty("infinispan.server.home.path");
         Path serverHomePath = serverHome == null ? Paths.get("") : Paths.get(serverHome);
         if (serverRoot == null) {
            serverRoot1 = serverHomePath.resolve("server");
         } else {
            serverRoot1 = serverHomePath.resolve(serverRoot);
         }
      }

      if (usersFile == null) {
         this.usersFile = serverRoot1.resolve("conf").resolve(DEFAULT_USERS_PROPERTIES_FILE);
      } else if (usersFile.isAbsolute()) {
         this.usersFile = usersFile;
      } else {
         this.usersFile = serverRoot1.resolve("conf").resolve(usersFile);
      }
      if (groupsFile == null) {
         this.groupsFile = serverRoot1.resolve("conf").resolve(DEFAULT_GROUPS_PROPERTIES_FILE);
      } else if (groupsFile.isAbsolute()) {
         this.groupsFile = groupsFile;
      } else {
         this.groupsFile = serverRoot1.resolve("conf").resolve(groupsFile);
      }
      load();
   }

   public void reload() {
      this.realm = null;
      this.encryption = Encryption.DEFAULT;
      load();
   }

   private void load() {
      if (Files.exists(usersFile)) {
         try (BufferedReader reader = Files.newBufferedReader(usersFile, StandardCharsets.UTF_8)) {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
               final String trimmed = currentLine.trim();
               if (trimmed.startsWith(COMMENT_PREFIX1) && trimmed.contains(REALM_COMMENT_PREFIX)) {
                  // this is the line that contains the realm name.
                  int start = trimmed.indexOf(REALM_COMMENT_PREFIX) + REALM_COMMENT_PREFIX.length();
                  int end = trimmed.indexOf(COMMENT_SUFFIX, start);
                  if (end > -1) {
                     realm = trimmed.substring(start, end);
                  }
               } else if (trimmed.startsWith(COMMENT_PREFIX1) && trimmed.contains(ALGORITHM_COMMENT_PREFIX)) {
                  // this is the line that contains the algorithm name.
                  int start = trimmed.indexOf(ALGORITHM_COMMENT_PREFIX) + ALGORITHM_COMMENT_PREFIX.length();
                  int end = trimmed.indexOf(COMMENT_SUFFIX, start);
                  if (end > -1) {
                     encryption = Encryption.valueOf(trimmed.substring(start, end).toUpperCase());
                  }
               } else {
                  if (!(trimmed.startsWith(COMMENT_PREFIX1) || trimmed.startsWith(COMMENT_PREFIX2))) {
                     String username = null;
                     StringBuilder builder = new StringBuilder();

                     CodePointIterator it = CodePointIterator.ofString(trimmed);
                     while (it.hasNext()) {
                        int cp = it.next();
                        if (cp == '\\' && it.hasNext()) { // escape
                           //might be regular escape of regex like characters \\t \\! or unicode \\uxxxx
                           int marker = it.next();
                           if (marker != 'u') {
                              builder.appendCodePoint(marker);
                           } else {
                              StringBuilder hex = new StringBuilder();
                              try {
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 builder.appendCodePoint((char) Integer.parseInt(hex.toString(), 16));
                              } catch (NoSuchElementException nsee) {
                                 throw Messages.MSG.invalidUnicodeSequence(hex.toString(), nsee);
                              }
                           }
                        } else if (username == null && (cp == '=' || cp == ':')) { // username-password delimiter
                           username = builder.toString().trim();
                           builder = new StringBuilder();
                        } else {
                           builder.appendCodePoint(cp);
                        }
                     }
                     if (username != null) { // end of line and delimiter was read
                        users.setProperty(username, builder.toString());
                     }
                  }
               }
            }
         } catch (IOException e) {
            throw MSG.userToolIOError(usersFile, e);
         }
      }
      if (Files.exists(groupsFile)) {
         try (Reader reader = Files.newBufferedReader(groupsFile)) {
            groups.load(reader);
         } catch (IOException e) {
            throw MSG.userToolIOError(groupsFile, e);
         }
      }
   }

   private void store() {
      store(this.realm, this.encryption);
   }

   private void store(String realm, Encryption encryption) {
      encryption = checkEncryption(encryption);
      if (realm == null) {
         realm = this.realm;
      }
      try (Writer writer = Files.newBufferedWriter(usersFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
         users.store(writer, REALM_COMMENT_PREFIX + realm + COMMENT_SUFFIX + "\n" + ALGORITHM_COMMENT_PREFIX + (encryption == Encryption.CLEAR ? "clear" : "encrypted") + COMMENT_SUFFIX);
      } catch (IOException e) {
         throw MSG.userToolIOError(usersFile, e);
      }
      try (Writer writer = Files.newBufferedWriter(groupsFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
         groups.store(writer, null);
      } catch (IOException e) {
         throw MSG.userToolIOError(groupsFile, e);
      }
   }

   private Encryption checkEncryption(Encryption encryption) {
      if (encryption == Encryption.DEFAULT) {
         // Not forcing anything, use what the current user.properties file specifies or the default
         return this.encryption;
      } else {
         if (this.encryption == Encryption.DEFAULT) {
            // We can override the default
            return encryption;
         } else if (this.encryption == encryption) {
            // Compatible
            return encryption;
         } else {
            throw MSG.userToolIncompatibleEncrypyion(encryption, this.encryption);
         }
      }
   }

   public String checkRealm(String realm) {
      if (realm == null) {
         return this.realm == null ? DEFAULT_REALM_NAME : this.realm;
      } else {
         if (this.realm == null || this.realm.equals(realm)) {
            return realm;
         } else {
            throw MSG.userToolWrongRealm(realm, this.realm);
         }
      }
   }

   public void createUser(String username, String password, String realm, Encryption encryption, List<String> userGroups, List<String> algorithms) {
      if (users.containsKey(username)) {
         throw MSG.userToolUserExists(username);
      }
      realm = checkRealm(realm);
      users.put(username, Encryption.CLEAR.equals(encryption) ? password : encryptPassword(username, realm, password, algorithms));
      groups.put(username, userGroups != null ? String.join(",", userGroups) : "");
      store(realm, encryption);
   }

   public String describeUser(String username) {
      if (users.containsKey(username)) {
         String[] userGroups = groups.containsKey(username) ? groups.getProperty(username).trim().split("\\s*,\\s*") : Util.EMPTY_STRING_ARRAY;
         return MSG.userDescribe(username, realm, userGroups);
      } else {
         throw MSG.userToolNoSuchUser(username);
      }
   }

   public void removeUser(String username) {
      users.remove(username);
      groups.remove(username);
      store();
   }

   public void modifyUser(String username, String password, String realm, Encryption encryption, List<String> userGroups, List<String> algorithms) {
      if (!users.containsKey(username)) {
         throw MSG.userToolNoSuchUser(username);
      } else {
         realm = checkRealm(realm);
         if (password != null) { // change password
            users.put(username, Encryption.CLEAR.equals(encryption) ? password : encryptPassword(username, realm, password, algorithms));
         }
         if (userGroups != null) { // change groups
            groups.put(username, String.join(",", userGroups));
         }
         store(realm, encryption);
      }
   }

   public void encryptAll(List<String> algorithms) {
      if (this.encryption == Encryption.CLEAR) {
         users.replaceAll((u, p) -> encryptPassword((String) u, realm, (String) p, algorithms));
         this.encryption = Encryption.ENCRYPTED;
         store(realm, Encryption.ENCRYPTED);
      }
   }

   private String encryptPassword(String username, String realm, String password, List<String> algorithms) {
      try {
         if (algorithms == null) {
            algorithms = DEFAULT_ALGORITHMS;
         }
         StringBuilder sb = new StringBuilder();
         for (String algorithm : algorithms) {
            PasswordFactory passwordFactory = PasswordFactory.getInstance(algorithm, WildFlyElytronPasswordProvider.getInstance());
            AlgorithmParameterSpec spec;
            sb.append(algorithm);
            sb.append(":");
            switch (algorithm) {
               case ScramDigestPassword.ALGORITHM_SCRAM_SHA_1:
               case ScramDigestPassword.ALGORITHM_SCRAM_SHA_256:
               case ScramDigestPassword.ALGORITHM_SCRAM_SHA_384:
               case ScramDigestPassword.ALGORITHM_SCRAM_SHA_512:
                  spec = new IteratedSaltedPasswordAlgorithmSpec(ScramDigestPassword.DEFAULT_ITERATION_COUNT, salt(ScramDigestPassword.DEFAULT_SALT_SIZE));
                  break;
               case DigestPassword.ALGORITHM_DIGEST_MD5:
               case DigestPassword.ALGORITHM_DIGEST_SHA:
               case DigestPassword.ALGORITHM_DIGEST_SHA_256:
               case DigestPassword.ALGORITHM_DIGEST_SHA_384:
               case DigestPassword.ALGORITHM_DIGEST_SHA_512:
                  spec = new DigestPasswordAlgorithmSpec(username, realm);
                  break;
               default:
                  throw MSG.userToolUnknownAlgorithm(algorithm);
            }
            Password encrypted = passwordFactory.generatePassword(new EncryptablePasswordSpec(password.toCharArray(), spec));
            byte[] encoded = BasicPasswordSpecEncoding.encode(encrypted, PROVIDERS);
            sb.append(ByteIterator.ofBytes(encoded).base64Encode().drainToString());
            sb.append(";");
         }
         return sb.toString();
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
         throw new RuntimeException(e);
      }
   }

   private static byte[] salt(int size) {
      byte[] salt = new byte[size];
      ThreadLocalRandom.current().nextBytes(salt);
      return salt;
   }

   public List<String> listUsers() {
      List<String> userList = new ArrayList<>(users.stringPropertyNames());
      Collections.sort(userList);
      return userList;
   }

   public List<String> listGroups() {
      return groups.values().stream()
            .map(o -> (String) o)
            .map(s -> s.split("\\s*,\\s*"))
            .flatMap(Arrays::stream)
            .filter(g -> !g.isEmpty())
            .sorted()
            .distinct()
            .collect(Collectors.toList());
   }

   public enum Encryption {
      DEFAULT,
      ENCRYPTED,
      CLEAR;

      public static Encryption valueOf(boolean plainText) {
         return plainText ? CLEAR : DEFAULT;
      }
   }
}
