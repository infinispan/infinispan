package org.infinispan.plugins.maven.defaults;

import static org.twdata.maven.mojoexecutor.MojoExecutor.artifactId;
import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;
import static org.twdata.maven.mojoexecutor.MojoExecutor.groupId;
import static org.twdata.maven.mojoexecutor.MojoExecutor.plugin;
import static org.twdata.maven.mojoexecutor.MojoExecutor.version;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;


/**
 * A maven plugin to extract default values from various AtributeDefinitions, output them to a specified properties/asciidoc
 * file and process xsd files so that placeholders are replaced with the extracted defaults.
 *
 * @author Ryan Emerson
 */
@Mojo(name = "extract-defaults",
      defaultPhase = LifecyclePhase.PROCESS_CLASSES,
      requiresDependencyResolution = ResolutionScope.RUNTIME)
public class DefaultsExtractorMojo extends AbstractMojo {

   private enum AttributeDefType {
      ISPN,
      SERVER,
      ALL
   }

   @Parameter(required = true, defaultValue = "${project.build.directory}/defaults.properties")
   private String defaultsFile;

   @Parameter(defaultValue = "ISPN")
   private AttributeDefType attributeDefType;

   @Parameter(defaultValue = "false")
   private boolean outputAscii;

   @Parameter(defaultValue = "true")
   private boolean filterXsd;

   @Parameter(defaultValue = "${project.basedir}/src/main/resources/schema")
   private String xsdSrcPath;

   @Parameter(defaultValue = "${project.build.directory}/classes/schema")
   private String xsdTargetPath;

   @Parameter
   private List<String> jars = new ArrayList<>();

   @Parameter(defaultValue = "${project}")
   private MavenProject mavenProject;

   @Parameter(defaultValue = "${session}")
   private MavenSession mavenSession;

   @Component
   private BuildPluginManager pluginManager;

   private DefaultsResolver ispnResovler = new InfinispanDefaultsResolver();
   private DefaultsResolver serverResovler = new ServerResourceDefaultsResolver();
   private List<String> classPaths;
   private ClassLoader classLoader;

   public void execute() throws MojoExecutionException {
      try {
         this.classPaths = mavenProject.getCompileClasspathElements();

         URL[] classLoaderUrls = classPaths.stream()
               .map(strPath -> FileSystems.getDefault().getPath(strPath))
               .map(this::pathToUrl)
               .toArray(URL[]::new);

         this.classLoader = new URLClassLoader(classLoaderUrls, Thread.currentThread().getContextClassLoader());
      } catch (DependencyResolutionRequiredException e) {
         throw new MojoExecutionException("Exception encountered during class extraction", e);
      }

      Set<Class> configClasses = new HashSet<>();
      getClassesFromClasspath(configClasses);
      jars.forEach(jar -> getClassesFromJar(jar, configClasses));
      Map<String, String> defaults = extractDefaults(configClasses);
      writeDefaultsToFile(defaults);

      if (filterXsd)
         filterXsdSchemas();
   }

   private boolean isValidClass(String className) {
      return ispnResovler.isValidClass(className) || serverResovler.isValidClass(className);
   }

   private Map<String, String> extractDefaults(Set<Class> classes) {
      String separator = outputAscii ? "-" : ".";
      Map<String, String> defaults = new HashMap<>();
      boolean extractAll = attributeDefType == AttributeDefType.ALL;
      if (extractAll || attributeDefType == AttributeDefType.ISPN)
         defaults.putAll(ispnResovler.extractDefaults(classes, separator));

      if (extractAll || attributeDefType == AttributeDefType.SERVER)
         defaults.putAll(serverResovler.extractDefaults(classes, separator));

      return defaults;
   }

   private URL pathToUrl(Path path) {
      try {
         return path.toUri().toURL();
      } catch (MalformedURLException ignore) {
         return null;
      }
   }

   private void getClassesFromClasspath(Set<Class> classes) throws MojoExecutionException {
      try {
         FileSystem fs = FileSystems.getDefault();
         PathMatcher classDir = fs.getPathMatcher("glob:*/**/target/classes");

         List<File> packageRoots = classPaths.stream()
               .map(fs::getPath)
               .filter(classDir::matches)
               .map(Path::toFile)
               .collect(Collectors.toList());

         for (File packageRoot : packageRoots)
            getClassesInPackage(packageRoot, "", classes);
      } catch (ClassNotFoundException e) {
         throw new MojoExecutionException("Exception encountered during class extraction", e);
      }
   }

   private void getClassesInPackage(File packageDir, String packageName, Set<Class> classes) throws ClassNotFoundException {
      if (packageDir.exists()) {
         for (File file : packageDir.listFiles()) {
            String fileName = file.getName();
            if (file.isDirectory()) {
               String subpackage = packageName.isEmpty() ? fileName : packageName + "." + fileName;
               getClassesInPackage(file, subpackage, classes);
            } else if (isValidClass(fileName)) {
               String className = fileName.substring(0, fileName.length() - 6);
               classes.add(Class.forName(packageName + "." + className, true, classLoader));
            }
         }
      }
   }

   private void getClassesFromJar(String jarName, Set<Class> classes) {
      // Ignore version number, necessary as jar is loaded differently when sub module is installed in isolation
      Optional<String> jarPath = classPaths.stream().filter(str -> str.contains(jarName)).findFirst();
      if (jarPath.isPresent()) {
         try {
            ZipInputStream jar = new ZipInputStream(new FileInputStream(jarPath.get()));
            for (ZipEntry entry = jar.getNextEntry(); entry != null; entry = jar.getNextEntry()) {
               if (!entry.isDirectory() && isValidClass(entry.getName())) {
                  String className = entry.getName().replace("/", ".");
                  classes.add(Class.forName(className.substring(0, className.length() - 6), true, classLoader));
               }
            }
         } catch (IOException | ClassNotFoundException e) {
            getLog().error(String.format("Unable to process jar '%s'", jarName), e);
         }
      } else {
         // We just warn here, as jars are required for `mvn install`, but not for `mvn test`
         getLog().info("Skipping Jar '" + jarName + "' as it cannot be found on the classpath");
      }
   }

   private void writeDefaultsToFile(Map<String, String> defaults) throws MojoExecutionException {
      File file = new File(defaultsFile);
      if (file.getParentFile() != null)
         file.getParentFile().mkdirs();

      try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
         defaults.entrySet().stream()
               .map(this::formatOutput)
               .sorted()
               .forEach(printWriter::println);
         printWriter.flush();
      } catch (IOException e) {
         throw new MojoExecutionException(String.format("Unable to write extracted defaults to the path '%s'", defaultsFile), e);
      }
   }

   private String formatOutput(Map.Entry<String, String> entry) {
      if (outputAscii) {
         return ":" + entry.getKey() + ": " + entry.getValue();
      }
      return entry.getKey() + " = " + entry.getValue();
   }

   private void filterXsdSchemas() throws MojoExecutionException {
      executeMojo(
            plugin(
                  groupId("org.apache.maven.plugins"),
                  artifactId("maven-resources-plugin"),
                  version("2.6")
            ),
            goal("copy-resources"),
            configuration(
                  element("overwrite", "true"),
                  element("outputDirectory", defaultsFile),
                  element("resources",
                        element("resource",
                              element("directory", xsdSrcPath),
                              element("targetPath", xsdTargetPath),
                              element("includes",
                                    element("include", "*.xsd")
                              ),
                              element("filtering", "true")
                        )
                  ),
                  element("filters",
                        element("filter", defaultsFile)
                  )
            ),
            executionEnvironment(
                  mavenProject,
                  mavenSession,
                  pluginManager
            )
      );
   }
}
