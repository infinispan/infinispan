#!/usr/bin/env groovy

pipeline {
    agent {
        label 'slave-group-normal'
    }

    options {
        timeout(time: 4, unit: 'HOURS')
    }

    stages {
        stage('Prepare') {
            steps {
                // Workaround for JENKINS-47230
                script {
                    env.MAVEN_HOME = tool('Maven')
                }

                sh returnStdout: true, script: 'cleanup.sh'
            }
        }

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    sh "$MAVEN_HOME/bin/mvn clean install -B -V -s $MAVEN_SETTINGS -DskipTests"
                }
                warnings canRunOnFailed: true, consoleParsers: [[parserName: 'Maven'], [parserName: 'Java Compiler (javac)']], shouldDetectModules: true
                checkstyle canRunOnFailed: true, pattern: '**/target/checkstyle-result.xml', shouldDetectModules: true
            }
        }

        stage('Tests') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    sh "$MAVEN_HOME/bin/mvn verify -B -V -s $MAVEN_SETTINGS -Dmaven.test.failure.ignore=true -Djansi.strip"
                }
                // TODO Add StabilityTestDataPublisher after https://issues.jenkins-ci.org/browse/JENKINS-42610 is fixed
                // Capture target/surefire-reports/*.xml, target/failsafe-reports/*.xml,
                // target/failsafe-reports-embedded/*.xml, target/failsafe-reports-remote/*.xml
                junit testResults: '**/target/*-reports*/*.xml',
                        testDataPublishers: [[$class: 'ClaimTestDataPublisher']],
                        healthScaleFactor: 100, allowEmptyResults: true

                // Workaround for SUREFIRE-1426: Fail the build if there a fork crashed
                script {
                    if (manager.logContains("org.apache.maven.surefire.booter.SurefireBooterForkException:.*")) {
                        echo "Fork error found"
                        manager.buildFailure()
                    }
                }

                // Dump any dump files to the console
                sh 'find . -name "*.dump*" -exec echo {} \\; -exec cat {} \\;'
                sh 'find . -name "hs_err_*" -exec echo {} \\; -exec grep "^# " {} \\;'
            }
        }
    }

    post {
        always {
            // Show the agent name in the build list
            script {
                // The manager variable requires the Groovy Postbuild plugin
                def matcher = manager.getLogMatcher("Running on (.*) in .*")
                if (matcher?.matches()) {
                    manager.addShortText(matcher.group(1), "grey", "", "0px", "")
                }
            }

            // Archive logs and dump files
            sh 'find . \\( -name "*.log" -o -name "*.dump*" -o -name "hs_err_*" \\) -exec xz {} \\;'
            archiveArtifacts allowEmptyArchive: true, artifacts: '**/*.xz'

            // Clean
            sh 'git clean -fdx || echo "git clean failed, exit code $?"'
        }
    }
}
