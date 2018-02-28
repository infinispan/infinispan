#!/usr/bin/env groovy

pipeline {
    agent {
        label 'slave-group-normal'
    }

    options {
        timeout(time: 3, unit: 'HOURS')
    }

    stages {
        stage('Prepare') {
            steps {
                // 
                properties([[$class: 'ScannerJobProperty', doNotScan: false], [$class: 'JobRestrictionProperty'], parameters([choice(choices: ['Oracle JDK 8', 'Oracle JDK 9', 'Oracle JDK 10', 'IBM JDK 8', 'Oracle JDK 7'], description: '', name: 'JDK')])])

                // Show the agent name in the build list
                script {
                    // The manager variable requires the Groovy Postbuild plugin
                    manager.addShortText(env.NODE_NAME, "grey", "", "0px", "")
                }

                // Workaround for JENKINS-47230
                script {
                    env.MAVEN_HOME = tool('Maven')
                    env.MAVEN_OPTS = "-Xmx800m -XX:+HeapDumpOnOutOfMemoryError"
                    env.JAVA_HOME = tool('${params.JDK}')
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
                sh 'env'
            }
        }
    }

    post {
    }
}
