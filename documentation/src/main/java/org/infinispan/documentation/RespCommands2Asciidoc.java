package org.infinispan.documentation;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.commands.FamilyCommand;

/**
 * Generates AsciiDoc documentation for RESP commands.
 * <p>
 * This tool reads all registered RESP commands and generates documentation
 * grouped by command family with ACL information.
 *
 * @since 16.2
 */
public class RespCommands2Asciidoc {

   // Data type categories used for grouping commands
   private static final AclCategory[] DATA_TYPE_CATEGORIES = {
         AclCategory.STRING,
         AclCategory.HASH,
         AclCategory.LIST,
         AclCategory.SET,
         AclCategory.SORTEDSET,
         AclCategory.BITMAP,
         AclCategory.HYPERLOGLOG,
         AclCategory.STREAM,
         AclCategory.PUBSUB,
         AclCategory.JSON,
         AclCategory.BLOOM,
         AclCategory.CUCKOO,
         AclCategory.CMS,
         AclCategory.TOPK,
         AclCategory.TDIGEST,
         AclCategory.SCRIPTING,
         AclCategory.TRANSACTION,
         AclCategory.CONNECTION,
         AclCategory.KEYSPACE,
         AclCategory.SEARCH
   };

   // Human-readable names for categories
   private static final Map<AclCategory, String> CATEGORY_NAMES = Map.ofEntries(
         Map.entry(AclCategory.STRING, "String Commands"),
         Map.entry(AclCategory.HASH, "Hash Commands"),
         Map.entry(AclCategory.LIST, "List Commands"),
         Map.entry(AclCategory.SET, "Set Commands"),
         Map.entry(AclCategory.SORTEDSET, "Sorted Set Commands"),
         Map.entry(AclCategory.BITMAP, "Bitmap Commands"),
         Map.entry(AclCategory.HYPERLOGLOG, "HyperLogLog Commands"),
         Map.entry(AclCategory.STREAM, "Stream Commands"),
         Map.entry(AclCategory.PUBSUB, "Pub/Sub Commands"),
         Map.entry(AclCategory.JSON, "JSON Commands"),
         Map.entry(AclCategory.BLOOM, "Bloom Filter Commands"),
         Map.entry(AclCategory.CUCKOO, "Cuckoo Filter Commands"),
         Map.entry(AclCategory.CMS, "Count-Min Sketch Commands"),
         Map.entry(AclCategory.TOPK, "Top-K Commands"),
         Map.entry(AclCategory.TDIGEST, "T-Digest Commands"),
         Map.entry(AclCategory.SCRIPTING, "Scripting Commands"),
         Map.entry(AclCategory.TRANSACTION, "Transaction Commands"),
         Map.entry(AclCategory.CONNECTION, "Connection Commands"),
         Map.entry(AclCategory.KEYSPACE, "Generic Commands"),
         Map.entry(AclCategory.SEARCH, "Search Commands")
   );

   public static void main(String[] args) throws IOException {
      if (args.length < 2) {
         System.err.println("Usage: RespCommands2Asciidoc <output-file> <notes-file>");
         System.exit(1);
      }

      Path outputPath = Path.of(args[0]);
      Path notesPath = Path.of(args[1]);

      Path parent = outputPath.getParent();
      if (parent != null) {
         Files.createDirectories(parent);
      }

      Properties notes = loadNotes(notesPath);

      try (PrintStream out = new PrintStream(Files.newOutputStream(outputPath))) {
         generateDocumentation(out, notes);
      }
   }

   private static Properties loadNotes(Path notesPath) throws IOException {
      Properties props = new Properties();
      if (Files.exists(notesPath)) {
         try (InputStream is = Files.newInputStream(notesPath)) {
            props.load(is);
         }
      }
      return props;
   }

   private static void generateDocumentation(PrintStream out, Properties notes) {
      out.println("[id='redis-commands_{context}']");
      out.println("= Redis commands");
      out.println();
      out.println("The {brandname} RESP endpoint implements the following Redis commands, grouped by category.");
      out.println();
      out.println("== ACL Categories");
      out.println();
      out.println("Each command shows its ACL categories which control access permissions:");
      out.println();
      out.println("* *Data type*: `@string`, `@hash`, `@list`, `@set`, `@sortedset`, `@bitmap`, `@hyperloglog`, `@stream`, `@pubsub`, `@json`, `@bloom`, `@cuckoo`, `@cms`, `@topk`, `@tdigest`");
      out.println("* *Operation*: `@read`, `@write`, `@keyspace`, `@admin`, `@dangerous`");
      out.println("* *Performance*: `@fast`, `@slow`, `@blocking`");
      out.println("* *Other*: `@connection`, `@transaction`, `@scripting`");
      out.println();

      List<CommandInfo> allCommands = collectCommands(notes);

      // Group commands by primary category
      Map<AclCategory, List<CommandInfo>> grouped = groupByCategory(allCommands);

      // Write each category section
      for (AclCategory category : DATA_TYPE_CATEGORIES) {
         List<CommandInfo> commands = grouped.get(category);
         if (commands != null && !commands.isEmpty()) {
            writeCategorySection(out, category, commands, notes);
         }
      }

      // Write uncategorized commands if any
      List<CommandInfo> uncategorized = grouped.get(null);
      if (uncategorized != null && !uncategorized.isEmpty()) {
         writeUncategorizedSection(out, uncategorized, notes);
      }
   }

   private static List<CommandInfo> collectCommands(Properties notes) {
      List<CommandInfo> commands = new ArrayList<>();

      for (RespCommand cmd : Commands.all()) {
         String name = cmd.getName();
         long aclMask = cmd.aclMask();

         if (cmd instanceof FamilyCommand familyCmd) {
            // Family command - expand subcommands
            RespCommand[] subCommands = familyCmd.getFamilyCommands();
            for (RespCommand subCmd : subCommands) {
               String subName = name + " " + subCmd.getName();
               // Subcommands inherit parent ACL mask, but may have their own
               long subAclMask = subCmd.aclMask() != 0 ? subCmd.aclMask() : aclMask;
               commands.add(new CommandInfo(subName, name, subCmd.getName(), subAclMask));
            }
         } else {
            // Check if this command has manually-specified subcommands (like CLIENT, MEMORY)
            String subcommandsKey = name + ".subcommands";
            String subcommands = notes.getProperty(subcommandsKey);
            if (subcommands != null && !subcommands.isEmpty()) {
               for (String sub : subcommands.split(",")) {
                  String subName = name + " " + sub.trim();
                  commands.add(new CommandInfo(subName, name, sub.trim(), aclMask));
               }
            } else {
               commands.add(new CommandInfo(name, null, null, aclMask));
            }
         }
      }

      return commands;
   }

   private static Map<AclCategory, List<CommandInfo>> groupByCategory(List<CommandInfo> commands) {
      Map<AclCategory, List<CommandInfo>> grouped = new LinkedHashMap<>();

      for (CommandInfo cmd : commands) {
         AclCategory primaryCategory = getPrimaryCategory(cmd.aclMask());
         grouped.computeIfAbsent(primaryCategory, k -> new ArrayList<>()).add(cmd);
      }

      // Sort commands within each category
      for (List<CommandInfo> list : grouped.values()) {
         list.sort(Comparator.comparing(CommandInfo::displayName));
      }

      return grouped;
   }

   private static AclCategory getPrimaryCategory(long aclMask) {
      // Find the first matching data type category
      for (AclCategory cat : DATA_TYPE_CATEGORIES) {
         if (cat.matches(aclMask)) {
            return cat;
         }
      }
      return null;
   }

   private static void writeCategorySection(PrintStream out, AclCategory category,
                                            List<CommandInfo> commands, Properties notes) {
      String sectionName = CATEGORY_NAMES.getOrDefault(category, category.name() + " Commands");
      out.println();
      out.printf("== %s%n", sectionName);
      out.println();

      // Write table
      out.println("[cols=\"2,1,3\", options=\"header\"]");
      out.println("|===");
      out.println("|Command |ACL |Description");
      out.println();

      for (CommandInfo cmd : commands) {
         writeCommandRow(out, cmd, notes);
      }

      out.println("|===");
   }

   private static void writeUncategorizedSection(PrintStream out, List<CommandInfo> commands, Properties notes) {
      out.println();
      out.println("== Other Commands");
      out.println();

      out.println("[cols=\"2,1,3\", options=\"header\"]");
      out.println("|===");
      out.println("|Command |ACL |Description");
      out.println();

      for (CommandInfo cmd : commands) {
         writeCommandRow(out, cmd, notes);
      }

      out.println("|===");
   }

   private static void writeCommandRow(PrintStream out, CommandInfo cmd, Properties notes) {
      String url = buildRedisUrl(cmd.displayName());

      // Command column with link and optional anchor
      if ("MULTI".equals(cmd.displayName())) {
         out.printf("|[[multi_command]]link:%s[%s]%n", url, cmd.displayName());
      } else {
         out.printf("|link:%s[%s]%n", url, cmd.displayName());
      }

      // ACL column - show operation categories (read/write/fast/slow/blocking)
      String aclBadges = getAclBadges(cmd.aclMask());
      out.printf("|%s%n", aclBadges);

      // Description column - note if present
      String noteKey = cmd.displayName().replace(" ", "_");
      String note = notes.getProperty(noteKey);
      if (note != null && !note.isEmpty()) {
         if (note.contains("\n")) {
            // Multi-line note: use AsciiDoc cell style for proper rendering
            out.printf("a|%s%n", note);
         } else {
            out.printf("|%s%n", note);
         }
      } else {
         out.println("|");
      }

      out.println();
   }

   private static String getAclBadges(long aclMask) {
      List<String> badges = new ArrayList<>();

      // Operation categories
      if (AclCategory.READ.matches(aclMask)) badges.add("`@read`");
      if (AclCategory.WRITE.matches(aclMask)) badges.add("`@write`");

      // Performance categories
      if (AclCategory.FAST.matches(aclMask)) badges.add("`@fast`");
      if (AclCategory.SLOW.matches(aclMask)) badges.add("`@slow`");
      if (AclCategory.BLOCKING.matches(aclMask)) badges.add("`@blocking`");

      // Special categories
      if (AclCategory.ADMIN.matches(aclMask)) badges.add("`@admin`");
      if (AclCategory.DANGEROUS.matches(aclMask)) badges.add("`@dangerous`");

      return badges.isEmpty() ? "-" : String.join(" ", badges);
   }

   private static String buildRedisUrl(String commandName) {
      // Convert command name to Redis URL format
      // e.g., "CLUSTER NODES" -> "cluster-nodes"
      // e.g., "JSON.GET" -> "json.get"
      String urlPath = commandName.toLowerCase()
            .replace(" ", "-")
            .replace("_", "-");
      return "https://redis.io/commands/" + urlPath + "/";
   }

   record CommandInfo(String displayName, String parentCommand, String subCommand, long aclMask) {
      boolean isSubCommand() {
         return parentCommand != null;
      }
   }
}
