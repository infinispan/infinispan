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
                // Show the agent name in the build list
                script {
                    // The manager variable requires the Groovy Postbuild plugin
                    manager.addShortText(env.NODE_NAME, "grey", "", "0px", "")
                }

                // Workaround for JENKINS-47230
                script {
                    env.MAVEN_HOME = tool('Maven')
                    env.MAVEN_OPTS = "-Xmx800m -XX:+HeapDumpOnOutOfMemoryError"
                    env.JAVA_HOME = tool('JDK 8')
                    env.JAVA10_HOME = tool('JDK 10')
                }

                sh 'cleanup.sh'
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
                    sh "$MAVEN_HOME/bin/mvn clean install -B -V -e -s $MAVEN_SETTINGS -DskipTests -Djava10.home=$JAVA10_HOME"
                }
                warnings canRunOnFailed: true, consoleParsers: [[parserName: 'Maven'], [parserName: 'Java Compiler (javac)']], shouldDetectModules: true
                checkstyle canRunOnFailed: true, pattern: '**/target/checkstyle-result.xml', shouldDetectModules: true
            }
        }

        stage('Tests') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    sh "$MAVEN_HOME/bin/mvn verify -B -V -e -s $MAVEN_SETTINGS -Dmaven.test.failure.ignore=true -Dansi.strip=true -Djava10.home=$JAVA10_HOME"
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
            // Archive logs and dump files
            sh 'find . \\( -name "*.log" -o -name "*.dump*" -o -name "hs_err_*" -o -name "*.hprof" \\) -exec xz {} \\;'
            archiveArtifacts allowEmptyArchive: true, artifacts: '**/*.xz,documentation/target/generated-html/**,**/surefire-reports/TEST-*.xml'

            // Clean
            sh 'git clean -qfdx || echo "git clean failed, exit code $?"'
        }

        changed {
            script {
              echo "post build status: changed"
              changed = true
            }
        }

        unstable {
            echo "post build status: unstable"
            script {
                echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                if (params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                    echo 'Sending notify'
                    emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                    body: '${DEFAULT_CONTENT}'
                }
            }
        }

        failure {
            echo "post build status: failure"
            script {
                echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                if (params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                    echo 'Sending notify'
                    emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                    body: '${DEFAULT_CONTENT}'
                }
            }
        }

        success {
            echo "post build status: success"
            script {
                echo "changed = ${changed}"
                if (changed) {
                    echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                    if ( params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                        echo 'Sending notify'
                        emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                        body: '${DEFAULT_CONTENT}'
                    }
                }
                if (!env.BRANCH_NAME.startsWith('PR-')) {
                    configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                        sh "$MAVEN_HOME/bin/mvn deploy:deploy -B -V -e -s $MAVEN_SETTINGS -Dmaven.test.failure.ignore=true -Dansi.strip -DskipTests=true"
                    }
                }
            }
        }
    }
}
