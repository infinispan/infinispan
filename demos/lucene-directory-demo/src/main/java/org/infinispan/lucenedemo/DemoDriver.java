package org.infinispan.lucenedemo;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;

/**
 * DemoDriver is a console hello-world application to show the Lucene
 * clustering capabilities.
 * This class parses the user input and drives the actions implemented in DemoActions.
 * 
 * As always when running JGroups to run a demo cluster of multiple applications running
 * on the same host, set these JVM options:
 * -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class DemoDriver implements Runnable {

   private final DemoActions actions;

   public DemoDriver(Directory infinispanDirectory, Cache<?, ?> cache) {
      actions = new DemoActions(infinispanDirectory, cache);
   }

   public static void main(String[] args) throws IOException {
      DefaultCacheManager cacheManager = new DefaultCacheManager("config-samples/lucene-demo-cache-config.xml");
      cacheManager.start();
      try {
         Cache<?, ?> cache = cacheManager.getCache();
         Directory directory = DirectoryBuilder
                  .newDirectoryInstance(cache, cache, cache, "index-name")
                  .create();
         DemoDriver driver = new DemoDriver(directory, cache);
         driver.run();
      }
      finally {
         cacheManager.stop();
      }
   }

   private void doQuery(Scanner scanner) {
      scanner.nextLine();
      Query query = null;
      while (query == null) {
         System.out.println("Enter a query:");
         String queryLine = scanner.nextLine();
         try {
            query = actions.parseQuery(queryLine);
         } catch (ParseException e) {
            System.out.println("Wrong syntax in query: " + e.getMessage());
            System.out.println("type it again: ");
         }
      }
      List<String> listMatches = actions.listStoredValuesMatchingQuery(query);
      printResult(listMatches);
   }

   private void insertNewText(Scanner scanner) throws IOException {
      System.out.println("Enter string as new document:");
      scanner.nextLine();
      String line = scanner.nextLine();
      actions.addNewDocument(line);
   }

   private void listAllDocuments() {
      List<String> listMatches = actions.listAllDocuments();
      printResult(listMatches);
   }

   private void listMembers() {
      List<Address> members = actions.listAllMembers();
      System.out.println("\tmembers:\t" + members);
   }

   private void showOptions() {
      System.out.println(
               "Options:\n" +
               "\t[1] List cluster members\n" +
               "\t[2] List all documents in index\n" +
               "\t[3] insert new text\n" +
               "\t[4] enter a query\n" +
               "\t[5] quit");
   }

   private void printResult(List<String> storedValues) {
      System.out.println("Matching documents:\n");
      if (storedValues.isEmpty()) {
         System.out.println("\tNo documents found.");
      }
      else {
         int i = 0;
         for (String value : storedValues) {
            System.out.println(++i + "\t\"" + value + "\"");
         }   
      }
   }

   @Override
   public void run() {
      Scanner scanner = new Scanner(System.in);
      while (true) {
         showOptions();
         boolean warned = false;
         while (!scanner.hasNextInt()) {
            if (!warned) {
               System.out.println("Invalid option, try again:");
               warned = true;
            }
            scanner.nextLine();
         }
         int result = scanner.nextInt();
         try {
            switch (result) {
               case 1:
                  listMembers();
                  break;
               case 2:
                  listAllDocuments();
                  break;
               case 3:
                  insertNewText(scanner);
                  break;
               case 4:
                  doQuery(scanner);
                  break;
               case 5:
                  System.out.println("Quit.");
                  return;
               default:
                  System.out.println("Invalid option.");
            }
            System.out.println("");
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

}
