package org.infinispan.test.fwk;

import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that tests that test names are correctly set for each test.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "test.fwk.TestNameVerifier")
public class TestNameVerifier {
    
   String dir = "src" + File.separator + "test" + File.separator + "java" + File.separator + "org" + File.separator + "infinispan";
   public List<String> modules = new ArrayList<String>();


   Pattern packageLinePattern = Pattern.compile("package org.infinispan[^;]*");
   Pattern classLinePattern = Pattern.compile("(abstract\\s*)??(public\\s*)(abstract\\s*)??class [^\\s]*");
   Pattern atAnnotationPattern = Pattern.compile("^\\s*@Test[^)]*");
   Pattern testNamePattern = Pattern.compile("testName\\s*=\\s*\"[^\"]*\"");

   String fileCache;

   FilenameFilter javaFilter = new FilenameFilter() {
      public boolean accept(File dir, String name) {
         return !dir.getAbsolutePath().contains("testng") && name.endsWith(".java");
      }
   };

   FileFilter onlyDirs = new FileFilter() {
      public boolean accept(File pathname) {
         return pathname.isDirectory();
      }
   };

   @Test(groups = "manual", description = "Do not enable this unless you want your files to be updated with test names!!!")
   public void process() throws Exception {
      File[] javaFiles = getAllJavaFiles();
      for (File file : javaFiles) {
         if (needsUpdate(file)) {
            System.out.println("Updating file: " + file.getAbsolutePath());
            updateFile(file);
         }
      }
   }

   private void updateFile(File file) throws Exception {
      String javaString = fileCache;
      String testName = getTestName(javaString, file.getName());
      String testNameStr = ", testName = \"" + testName + "\"";
      javaString = replaceAtTestAnnotation(javaString, testNameStr);
      persistNewFile(file, javaString);
   }

   private void persistNewFile(File file, String javaString) throws Exception {
      if (file.delete()) {
         System.out.println("!!!!!!!!!! error processing file " + file.getName());
         return;
      }
      file.createNewFile();
      PrintWriter writter = new PrintWriter(file);
      writter.append(javaString);
      writter.close();
   }

   private String replaceAtTestAnnotation(String javaString, String testNameStr) {
      Matcher matcher = atAnnotationPattern.matcher(javaString);
      boolean found = matcher.find();
      assert found;
      String theMatch = matcher.group();
      return matcher.replaceFirst(theMatch + testNameStr);
   }

   private String getTestName(String javaString, String filename) {
      String classNamePart = getClassNamePart(javaString, filename);

      //abstract classes do not require test names
      if (classNamePart.indexOf("abstract") >= 0) return null;

      classNamePart = classNamePart.substring("public class ".length());
      String packagePart = getPackagePart(javaString, filename);
      //if the test is in org.infinispan package then make sure no . is prepended
      String packagePrepend = ((packagePart != null) && (packagePart.length() > 0)) ? packagePart + "." : "";
      return packagePrepend + classNamePart;
   }

   private String getClassNamePart(String javaString, String filename) {
      Matcher matcher = classLinePattern.matcher(javaString);
      boolean found = matcher.find();
      assert found : "could not determine class name for file: " + filename;
      return matcher.group();
   }

   private String getPackagePart(String javaString, String filename) {
      Matcher matcher = packageLinePattern.matcher(javaString);
      boolean found = matcher.find();
      assert found : "Could not determine package name for file: " + filename;
      String theMatch = matcher.group();
      String partial = theMatch.substring("package org.infinispan".length());
      if (partial.trim().length() == 0) return partial.trim();
      return partial.substring(1);//drop the leading dot.
   }


   private boolean needsUpdate(File file) throws Exception {
      String javaFileStr = getFileAsString(file);
      if (javaFileStr.indexOf(" testName = \"") > 0) return false;
      int atTestIndex = javaFileStr.indexOf("@Test");
      int classDeclarationIndex = javaFileStr.indexOf("public class");
      return atTestIndex > 0 && atTestIndex < classDeclarationIndex;
   }

   private String getFileAsString(File file) throws Exception {
      StringBuilder builder = new StringBuilder();
      BufferedReader fileReader = new BufferedReader(new FileReader(file));
      String line;
      while ((line = fileReader.readLine()) != null) {
         builder.append(line + "\n");
      }
      this.fileCache = builder.toString();
      return fileCache;
   }

   // Loop through the list of module names and pass it to the getFilesFromModule()
   private File[] getAllJavaFiles() {
      populateModuleList();
      List<File> listOfFiles = new ArrayList<File>();
      for (String module : modules) {
         // Take in the list from the getFilesFromModule() and add it to the collection here to be converted into an
         // array and then returned.
         listOfFiles.addAll(getFilesFromModule(module));
      }
      return listOfFiles.toArray(new File[listOfFiles.size()]);
   }

   private void addJavaFiles(File file, ArrayList<File> result) {
      assert file.isDirectory();
      File[] javaFiles = file.listFiles(javaFilter);
//      printFiles(javaFiles);
      result.addAll(Arrays.asList(javaFiles));
      for (File dir : file.listFiles(onlyDirs)) {
         addJavaFiles(dir, result);
      }
   }

   public void verifyTestName() throws Exception {
      File[] javaFiles = getAllJavaFiles();
      StringBuilder errorMessage = new StringBuilder("Following test class(es) do not have an appropriate test names: \n");
      boolean hasErrors = false;
      for (File file : javaFiles) {
         String expectedName = incorrectTestName(file);
         if (expectedName != null) {
            errorMessage.append(file.getAbsoluteFile()).append(" (Expected test name '").append(expectedName).append("', was '").append(existingTestName(file)).append("'\n");
            hasErrors = true;
         }
      }
      assert !hasErrors : errorMessage.append("The rules for writing unit tests are described on http://www.jboss.org/community/wiki/ParallelTestSuite");
   }

   private String incorrectTestName(File file) throws Exception {
      String fileAsStr = getFileAsString(file);

      boolean containsTestAnnotation = atAnnotationPattern.matcher(fileAsStr).find();
      if (!containsTestAnnotation) return null;

      String expectedTestName = getTestName(fileAsStr, file.getName());
      if (expectedTestName == null) return null; //this happens when the class is abstract

      String existingTestName = existingTestName(file);
      if (existingTestName == null || !existingTestName.equals(expectedTestName)) return expectedTestName;
      return null;
   }

   private String existingTestName(File file) throws Exception {
      String fileAsStr = getFileAsString(file);

      boolean containsTestAnnotation = atAnnotationPattern.matcher(fileAsStr).find();
      if (!containsTestAnnotation) return null;

      String expectedTestName = getTestName(fileAsStr, file.getName());
      if (expectedTestName == null) return null; //this happens when the class is abstract
      Matcher matcher = this.testNamePattern.matcher(fileAsStr);
      if (!matcher.find()) return expectedTestName;
      String name = matcher.group().trim();
      int firstIndexOfQuote = name.indexOf('"');
      return name.substring(firstIndexOfQuote + 1, name.length() - 1);
   }

   // method that populates the list of module names
   private void populateModuleList() {
      modules.add("core");
      modules.add("cachestore" + File.separator + "jdbc");
      modules.add("cachestore" + File.separator + "remote");
      modules.add("rhq-plugin");
      modules.add("tree");
   }

   // Old method that Mircea wrote that originally returned an array. Now returns a list and will be added to the list in getAllJavaFiles()
   private List<File> getFilesFromModule(String moduleName) {
      File file = new File(new File(dir).getAbsolutePath());
      assert file.isDirectory();
      ArrayList<File> result = new ArrayList<File>();
      addJavaFiles(file, result);
      return result;
   }
}
