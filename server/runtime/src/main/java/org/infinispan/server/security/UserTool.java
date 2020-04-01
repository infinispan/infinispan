package org.infinispan.server.security;

import static org.infinispan.server.logging.Messages.MSG;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Version;
import org.infinispan.server.Server;
import org.infinispan.server.tool.Main;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class UserTool extends Main {
   public static final String DEFAULT_USERS_PROPERTIES_FILE = "users.properties";
   public static final String DEFAULT_GROUPS_PROPERTIES_FILE = "groups.properties";
   public static final String DEFAULT_REALM_NAME = "default";

   private String username = null;
   private String password = null;
   private String realm = DEFAULT_REALM_NAME;
   private String usersFileName = DEFAULT_USERS_PROPERTIES_FILE;
   private String groupsFileName = DEFAULT_GROUPS_PROPERTIES_FILE;
   private List<String> addGroups = new ArrayList<>();
   private boolean batchMode = false;
   private boolean plainText = true;

   public static void main(String... args) {
      UserTool userTool = new UserTool();
      userTool.run(args);
   }

   protected void handleArgumentCommand(String command, String parameter, Iterator<String> args) {
      switch (command) {
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
         case "-d":
         case "--digest":
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
            throw new RuntimeException(e);
         }
      }
      Properties groups = new Properties();
      File groupsFile = new File(confDir, groupsFileName);
      if (groupsFile.exists()) {
         try (FileReader reader = new FileReader(groupsFile)) {
            groups.load(reader);
         } catch (IOException e) {
            throw new RuntimeException(e);
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
      users.put(username, plainText ? password : hashPassword(username, password, realm));
      groups.put(username, addGroups.stream().collect(Collectors.joining(",")));

      try (FileWriter writer = new FileWriter(usersFile)) {
         users.store(writer, "$REALM_NAME=" + realm + "$");
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      try (FileWriter writer = new FileWriter(groupsFile)) {
         groups.store(writer, null);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private static String hashPassword(String username, String password, String realm) {
      try {
         MessageDigest md5 = MessageDigest.getInstance("MD5");
         byte[] hashed = md5.digest((username + ":" + realm + ":" + password).getBytes(StandardCharsets.UTF_8));
         return toHex(hashed);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static String toHex(byte[] bytes) {
      StringBuilder sb = new StringBuilder();
      for (byte b : bytes) {
         sb.append(String.format("%02x", b));
      }
      return sb.toString();
   }

   public void help(PrintStream out) {
      out.printf("Usage:\n");
      out.printf("  -u, --user=<name>                  %s\n", MSG.userToolHelpUser());
      out.printf("  -p, --password=<password>          %s\n", MSG.userToolHelpPassword());
      out.printf("  -d, --digest                       %s\n", MSG.userToolHelpDigestPassword());
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
      out.printf("%s User Tool %s (%s)\n", Version.getBrandName(), Version.getVersion(), Version.getCodename());
      out.println("Copyright (C) Red Hat Inc. and/or its affiliates and other contributors");
      out.println("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0");
   }
}
