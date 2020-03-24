package org.infinispan.server.security;

import static org.infinispan.server.logging.Messages.MSG;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Version;
import org.infinispan.server.Server;
import org.infinispan.server.tool.Main;
import org.wildfly.common.iteration.ByteIterator;
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
public class UserTool extends Main {
   public static final String DEFAULT_USERS_PROPERTIES_FILE = "users.properties";
   public static final String DEFAULT_GROUPS_PROPERTIES_FILE = "groups.properties";
   public static final String DEFAULT_REALM_NAME = "default";

   private static final List<String> DEFAULT_ALGORITHMS = Arrays.asList(
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

   private String username = null;
   private String password = null;
   private String realm = DEFAULT_REALM_NAME;
   private String usersFileName = DEFAULT_USERS_PROPERTIES_FILE;
   private String groupsFileName = DEFAULT_GROUPS_PROPERTIES_FILE;
   private List<String> addGroups = new ArrayList<>();
   private List<String> algorithms = DEFAULT_ALGORITHMS;
   private boolean batchMode = false;
   private boolean plainText = false;

   public static void main(String... args) {
      UserTool userTool = new UserTool();
      userTool.run(args);
   }

   public UserTool() {
      SecurityActions.addSecurityProvider(WildFlyElytronPasswordProvider.getInstance());
   }

   protected void handleArgumentCommand(String command, String parameter, Iterator<String> args) {
      switch (command) {
         case "-a":
            parameter = args.next();
         case "--algorithms":
            algorithms = Arrays.stream(parameter.split(",")).collect(Collectors.toList());
            break;
         case "-b":
         case "--batch-mode":
            batchMode = true;
            break;
         case "-f":
            parameter = args.next();
            // Fall through
         case "--users-file":
            usersFileName = parameter;
            break;
         case "-p":
            parameter = args.next();
            // Fall through
         case "--password":
            password = parameter;
            break;
         case "-c":
         case "--clear-text":
            plainText = true;
            break;
         case "-e":
         case "--encrypted":
            plainText = false;
            break;
         case "-g":
            parameter = args.next();
            // Fall through
         case "--groups":
            for (String group : parameter.split(",")) {
               addGroups.add(group);
            }
            break;
         case "-w":
            parameter = args.next();
            // Fall through
         case "--groups-file":
            groupsFileName = parameter;
            break;
         case "-r":
            parameter = args.next();
            // Fall through
         case "--realm":
            realm = parameter;
            break;
         case "-s":
            parameter = args.next();
            // Fall through
         case "--server-root":
            serverRoot = new File(parameter);
            break;
         case "-u":
            parameter = args.next();
            // Fall through
         case "--user":
            username = parameter;
            break;
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   protected void runInternal() {
      while (username == null || username.isEmpty()) {
         username = System.console().readLine(MSG.userToolUsername());
      }
      File confDir = new File(serverRoot, Server.DEFAULT_SERVER_CONFIG);
      Properties users = new Properties();
      File usersFile = new File(confDir, usersFileName);
      if (usersFile.exists()) {
         try (FileReader reader = new FileReader(usersFile)) {
            users.load(reader);
         } catch (IOException e) {
            throw MSG.userToolIOError(usersFile, e);
         }
      }
      Properties groups = new Properties();
      File groupsFile = new File(confDir, groupsFileName);
      if (groupsFile.exists()) {
         try (FileReader reader = new FileReader(groupsFile)) {
            groups.load(reader);
         } catch (IOException e) {
            throw MSG.userToolIOError(groupsFile, e);
         }
      }
      if (!batchMode && users.containsKey(username)) {
         String answer;
         do {
            answer = System.console().readLine(MSG.userToolUserExists(username));
         } while (!"Y".equalsIgnoreCase(answer) && !"N".equalsIgnoreCase(answer));
         if ("N".equalsIgnoreCase(answer)) {
            exit(0);
         }
      }
      if (password == null) {
         if (!batchMode) {
            String confirm;
            do {
               while (password == null || password.isEmpty()) {
                  password = new String(System.console().readPassword(MSG.userToolPassword()));
               }
               confirm = new String(System.console().readPassword(MSG.userToolPasswordConfirm()));
               if (!password.equals(confirm)) {
                  password = null;
               }
            } while (password == null);
         } else {
            stdErr.println(MSG.userToolNoPassword(username));
            exit(1);
         }
      }
      users.put(username, plainText ? password : encryptPassword(username, realm, password));
      groups.put(username, String.join(",", addGroups));

      try (FileWriter writer = new FileWriter(usersFile)) {
         String algorithm = plainText ? "clear" : "encrypted";
         users.store(writer, "$REALM_NAME=" + realm + "$\n$ALGORITHM=" + algorithm + "$");
      } catch (IOException e) {
         throw MSG.userToolIOError(usersFile, e);
      }
      try (FileWriter writer = new FileWriter(groupsFile)) {
         groups.store(writer, null);
      } catch (IOException e) {
         throw MSG.userToolIOError(groupsFile, e);
      }
   }

   private String encryptPassword(String username, String realm, String password) {
      try {
         StringBuilder sb = new StringBuilder();
         for (String algorithm : algorithms) {
            PasswordFactory passwordFactory = PasswordFactory.getInstance(algorithm);
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
            byte[] encoded = BasicPasswordSpecEncoding.encode(encrypted);
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

   public void help(PrintStream out) {
      out.printf("Usage:\n");
      out.printf("  -u, --user=<name>                  %s\n", MSG.userToolHelpUser());
      out.printf("  -p, --password=<password>          %s\n", MSG.userToolHelpPassword());
      out.printf("  -c, --clear-text                   %s\n", MSG.userToolHelpClearTextPassword());
      out.printf("  -a, --algorithms=<algorithms>      %s\n", MSG.userToolHelpAlgorithms(DEFAULT_ALGORITHMS));
      out.printf("  -e, --encrypted                    %s\n", MSG.userToolHelpEncryptedPassword());
      out.printf("  -g, --groups=<group1[,group2...]>  %s\n", MSG.userToolHelpGroups());
      out.printf("  -f, --users-file=<file>            %s\n", MSG.userToolHelpUsersFile(DEFAULT_USERS_PROPERTIES_FILE));
      out.printf("  -w, --groups-file=<file>           %s\n", MSG.userToolHelpGroupsFile(DEFAULT_GROUPS_PROPERTIES_FILE));
      out.printf("  -r, --realm=<realm>                %s\n", MSG.userToolHelpRealm(DEFAULT_REALM_NAME));
      out.printf("  -s, --server-root=<path>           %s\n", MSG.toolHelpServerRoot(Server.DEFAULT_SERVER_ROOT_DIR));
      out.printf("  -b, --batch-mode                   %s\n", MSG.userToolHelpBatchMode());
      out.printf("  -h, --help                         %s\n", MSG.toolHelpHelp());
      out.printf("  -v, --version                      %s\n", MSG.toolHelpVersion());
   }

   @Override
   public void version(PrintStream out) {
      out.printf("%s User Tool %s\n", Version.getBrandName(), Version.getBrandVersion());
      out.println("Copyright (C) Red Hat Inc. and/or its affiliates and other contributors");
      out.println("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0");
   }
}
