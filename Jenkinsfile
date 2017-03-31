#!/usr/bin/env groovy

pipeline {
    agent any
    stages {
        stage('SCM Checkout') {
            steps {
                checkout scm
            }
        }
        
        stage('Build') {
            steps {
                script {
                  def mvnHome = tool 'Maven'
                  sh "${mvnHome}/bin/mvn clean package -pl bom -pl license -pl commons"
                }
            }
        }
        
        stage('Deploy SNAPSHOT') {
            when {
                branch 'master'
            }
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-with-deploy-snapshot', variable: 'MAVEN_SETTINGS')]) {
                    script {
                        def mvnHome = tool 'Maven'
                        sh "${mvnHome}/bin/mvn deploy -s $MAVEN_SETTINGS -pl bom -pl license -pl commons"
                    }  
                }
            }
        }
    }
    post {
        always {
            junit '**/target/*-reports/*.xml'
        }
    }
}
