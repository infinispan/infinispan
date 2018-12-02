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
                    env.JAVA_HOME = tool('JDK 8')
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
            }
        }

        stage('Main tests') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    sh "$MAVEN_HOME/bin/mvn verify -B -V -s $MAVEN_SETTINGS -Dmaven.test.failure.ignore=true"
                }
                // TODO Add StabilityTestDataPublisher after https://issues.jenkins-ci.org/browse/JENKINS-42610 is fixed
                // Capture target/surefire-reports/*.xml, target/failsafe-reports/*.xml,
                // target/failsafe-reports-embedded/*.xml, target/failsafe-reports-remote/*.xml
                junit testResults: '**/target/*-reports*/*.xml',
                        testDataPublishers: [[$class: 'ClaimTestDataPublisher']],
                        healthScaleFactor: 100
            }
        }

        stage('X-Site tests') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    sh "$MAVEN_HOME/bin/mvn verify -B -V -s $MAVEN_SETTINGS -pl core -Ptest-xsite -Dinfinispan.module-suffix=xsite -Dmaven.test.failure.ignore=true"
                }
                // TODO Add StabilityTestDataPublisher after https://issues.jenkins-ci.org/browse/JENKINS-42610 is fixed
                // Capture target/surefire-reports/*.xml, target/failsafe-reports/*.xml,
                // target/failsafe-reports-embedded/*.xml, target/failsafe-reports-remote/*.xml
                junit testResults: '**/target/*-reports*/*.xml',
                        testDataPublishers: [[$class: 'ClaimTestDataPublisher']],
                        healthScaleFactor: 100
            }
        }
    }
}
