#!/usr/bin/env groovy

pipeline {
    agent any
    stages {
        stage('Prepare') {
            steps {
                script {
                    sh returnStdout: true, script: 'cleanup.sh'
                }
            }
        }
        
        stage('SCM Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    script {
                        def mvnHome = tool 'Maven'
                        sh "${mvnHome}/bin/mvn clean install -s $MAVEN_SETTINGS -Dmaven.test.failure.ignore=true"
                        junit testDataPublishers: [[$class: 'ClaimTestDataPublisher']], testResults: '**/target/*-reports/*.xml'
                    }
                }
            }
        }
    }
}
