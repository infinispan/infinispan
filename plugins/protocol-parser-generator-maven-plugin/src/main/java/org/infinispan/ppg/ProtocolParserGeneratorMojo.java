package org.infinispan.ppg;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.infinispan.ppg.generator.GeneratorException;
import org.infinispan.ppg.generator.Grammar;
import org.infinispan.ppg.generator.Machine;
import org.infinispan.ppg.generator.Parser;
import org.infinispan.ppg.generator.ParserException;

@Mojo(name = "generate", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ProtocolParserGeneratorMojo extends AbstractMojo {

    @Parameter(property = "infinispan.ppg.outputDirectory", defaultValue = "${project.build.directory}/generated-sources/java")
    private File outputDirectory;

    @Parameter(required = true)
    private List<File> files;

    @Parameter(property = "infinispan.ppg.sourceDirectories", defaultValue = "${project.build.sourceDirectory}")
    private List<File> sourceDirectories;

    @Parameter(property = "infinispan.ppg.maxSwitchStates", defaultValue = "64")
    private int maxSwitchStates;

    @Parameter(property = "infinispan.ppg.userSwitchThreshold", defaultValue = "8")
    private int userSwitchThreshold;

    @Parameter(property = "infinispan.ppg.passContext", defaultValue = "false")
    private boolean passContext;

    @Parameter(readonly = true, defaultValue = "${project}")
    private MavenProject project;

    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info(String.format("Transforming %d handlers to %s", files.size(), outputDirectory));
        for (File file : files) {
            getLog().info(String.format("Transforming %s...", file));
            if (!file.exists()) {
                throw new MojoFailureException(String.format("Grammar file %s does not exist!", file));
            } else if (!file.isFile()) {
                throw new MojoFailureException(String.format("Grammar file %s is not a regular file!", file));
            }
            Grammar grammar;
            Machine machine;
            try {
                grammar = new Parser(getLog()::debug, sourceDirectories).load(file);
                machine = grammar.build(maxSwitchStates, userSwitchThreshold, passContext);
            } catch (IOException e) {
                throw new MojoExecutionException(String.format("Cannot read file %s", file), e);
            } catch (ParserException | GeneratorException e) {
                throw new MojoFailureException(String.format("Error in file %s", file), e);
            }
            File output = new File(outputDirectory, grammar.getPackage().replace('.', File.separatorChar) + File.separator + grammar.getSimpleName() + ".java");
            output.getParentFile().mkdirs();
            try {
                Files.write(Paths.get(output.toURI()), machine.buildSource().getBytes());
            } catch (IOException e) {
                throw new MojoExecutionException(String.format("Cannot write generated source code to %s", output), e);
            }
            project.addCompileSourceRoot(outputDirectory.getAbsolutePath());
        }
    }
}
