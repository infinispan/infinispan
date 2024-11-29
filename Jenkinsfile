#!/usr/bin/env groovy

pipeline {
    agent {
        label 'slave-group-normal || slave-group-k8s'
    }

    environment {
      TOKEN = credentials('jira-infinspan-bot')
    }

    parameters {
        choice(name: 'TEST_JDK', choices: ['Default', 'JDK 17', 'JDK 21', 'JDK latest'], description: 'The JDK used to run tests')
    }

    options {
        timeout(time: 4, unit: 'HOURS')
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '100', daysToKeepStr: '61'))
    }

    stages {
        stage('Prepare') {
            steps {
                script {
                    env.MAVEN_HOME = tool('Maven')
                    env.MAVEN_OPTS = "-Xmx1500m -XX:+HeapDumpOnOutOfMemoryError"
                    env.JAVA_HOME = tool('JDK 21')
                    env.GRAALVM_HOME = tool('GraalVM 21')
                    if (params.TEST_JDK != 'Default') {
                        env.JAVA_ALT_HOME = tool(params.TEST_JDK)
                        env.ALT_TEST_BUILD = "-Pjava-alt-test"
                    } else {
                        env.ALT_TEST_BUILD = ""
                    }
                    // ISPN-9703 Ensure distribution build works on non-prs
                    env.DISTRIBUTION_BUILD = !env.BRANCH_NAME.startsWith('PR-') || pullRequest.labels.contains('Documentation') || pullRequest.labels.contains('Image Required') ? "-Pdistribution" : ""
                    // Collect reports on non-prs
                    env.REPORTS_BUILD = env.BRANCH_NAME.startsWith('PR-') ? "" : "surefire-report:report pmd:cpd pmd:pmd spotbugs:spotbugs"
                    env.HIBERNATE_MATRIX = env.BRANCH_NAME.startsWith('PR-') ? "" : "-Phibernate-matrix"
                }
                echo "-----------"
                echo env.NODE_NAME
                echo "-----------"
                sh "$JAVA_HOME/bin/java --version"
                echo "-----------"
                sh "$MAVEN_HOME/bin/mvn --version"
                echo "-----------"
                sh "$GRAALVM_HOME/bin/java --version"
                echo "-----------"

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
                sh "$MAVEN_HOME/bin/mvn clean install $REPORTS_BUILD -B -e -DskipTests -Pnative $DISTRIBUTION_BUILD"
            }
        }

        stage('Deploy snapshot') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    // from Jenkins docs: Note that the build result can only get worse, so you cannot change the result to SUCCESS if the current result is UNSTABLE or worse
                    script {
                        if (!env.BRANCH_NAME.startsWith('PR-')) {
                            sh "$MAVEN_HOME/bin/mvn deploy -B -e -Pdistribution -Drelease-mode=upstream -DdeployServerZip=true -DskipTests -Dcheckstyle.skip=true -Dlicense.skipDownloadLicenses=true"
                        }
                    }
                }
            }
        }

        stage('Image') {
            when {
                expression {
                    return !env.BRANCH_NAME.startsWith("PR-") || pullRequest.labels.contains('Image Required')
                }
            }
            steps {
                script {
                    def mvnCmd = '-q -Dexec.executable=echo -Dexec.args=\'${project.version}\' --non-recursive exec:exec'
                    def SERVER_VERSION = sh(
                            script: "${MAVEN_HOME}/bin/mvn ${mvnCmd}",
                            returnStdout: true
                    ).trim()
                    def REPO = 'quay.io/infinispan-test/server'
                    def TAG = env.BRANCH_NAME
                    def IMAGE_BRANCH = env.CHANGE_ID ? pullRequest.base : env.BRANCH_NAME

                    sh "rm -rf infinispan-images"
                    sh "git clone --single-branch --branch ${IMAGE_BRANCH} --depth 1 https://github.com/infinispan/infinispan-images.git"


                    dir('infinispan-images') {
                        sh "cekit -v --descriptor server-openjdk.yaml build --overrides '{\"name\":\"${REPO}\", \"version\":\"${TAG}\"}' --overrides '{\"artifacts\":[{\"name\":\"server\",\"path\":\"../distribution/target/distribution/infinispan-server-${SERVER_VERSION}.zip\"}]}' docker\n"

                        withDockerRegistry(credentialsId: 'Quay-InfinispanTest', url: 'https://quay.io') {
                            sh "docker push ${REPO}:${TAG}"
                        }
                        sh "docker rmi ${REPO}:${TAG}"
                        deleteDir()
                    }

                    // CHANGE_ID is set only for pull requests, so it is safe to access the pullRequest global variable
                    if (env.CHANGE_ID) {
                        pullRequest.comment("Image pushed for Jenkins build [#${env.BUILD_NUMBER}](${env.BUILD_URL}):\n```\n${REPO}:${TAG}\n```")
                    }
                }
            }
        }

        stage('Native Image') {
            when {
                expression {
                    return !env.BRANCH_NAME.startsWith("PR-") || pullRequest.labels.contains('Native Image Required')
                }
            }
            steps {
                script {
                    def mvnCmd = '-q -Dexec.executable=echo -Dexec.args=\'${project.version}\' --non-recursive exec:exec'
                    def SERVER_VERSION = sh(
                            script: "${MAVEN_HOME}/bin/mvn ${mvnCmd}",
                            returnStdout: true
                    ).trim()
                    def REPO = 'quay.io/infinispan-test/server-native'
                    def TAG = env.BRANCH_NAME
                    def IMAGE_BRANCH = env.CHANGE_ID ? pullRequest.base : env.BRANCH_NAME

                    sh "rm -rf infinispan-images"
                    sh "git clone --single-branch --branch ${IMAGE_BRANCH} --depth 1 https://github.com/infinispan/infinispan-images.git"


                    dir('infinispan-images') {
                        sh "cekit -v --descriptor server-dev-native.yaml build --overrides '{\"name\":\"${REPO}\", \"version\":\"${TAG}\"}' --overrides '{\"artifacts\":[{\"name\":\"server\",\"path\":\"../quarkus/server-runner/target/infinispan-quarkus-server-runner-${SERVER_VERSION}-runner\"},{\"name\":\"cli\",\"path\":\"../quarkus/cli/target/infinispan-cli\"}]}' docker\n"

                        withDockerRegistry(credentialsId: 'Quay-InfinispanTest', url: 'https://quay.io') {
                            sh "docker push ${REPO}:${TAG}"
                        }
                        sh "docker rmi ${REPO}:${TAG}"
                        deleteDir()
                    }

                    // CHANGE_ID is set only for pull requests, so it is safe to access the pullRequest global variable
                    if (env.CHANGE_ID) {
                        pullRequest.comment("Image pushed for Jenkins build [#${env.BUILD_NUMBER}](${env.BUILD_URL}):\n```\n${REPO}:${TAG}\n```")
                    }
                }
            }
        }

        stage('Tests') {
            steps {
                timeout(time: 180, unit: 'MINUTES') {
                    sh "$MAVEN_HOME/bin/mvn verify -B -e -DrerunFailingTestsCount=2 -Dmaven.test.failure.ignore=true -Dansi.strip=true  -Dlicense.skipDownloadLicenses=true -Pnative $HIBERNATE_MATRIX $ALT_TEST_BUILD"
                }
                // Remove any default TestNG report files as this will result in tests being counted twice by Jenkins statistics
                sh "rm -rf **/target/*-reports*/**/TEST-TestSuite.xml"
                // TODO Add StabilityTestDataPublisher after https://issues.jenkins-ci.org/browse/JENKINS-42610 is fixed
                // Capture target/surefire-reports/*.xml, target/failsafe-reports/*.xml,
                // target/failsafe-reports-embedded/*.xml, target/failsafe-reports-remote/*.xml
                script {
                   def testResults = sh(
                     script: 'shopt -s nullglob globstar; ls **/target/*-reports*/**/TEST-*.xml | grep -v "FLAKY.xml" | paste -s -d, -',
                     returnStdout: true
                   ).trim()
                   junit testResults: "${testResults}",
                       testDataPublishers: [[$class: 'ClaimTestDataPublisher']],
                       healthScaleFactor: 100, allowEmptyResults: true
                }
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
            // Record any warnings before the tests log their own stuff
            recordIssues enabledForFailure: true,
                         skipBlames: true,
                         quiet: true,
                         tools: [
                             mavenConsole(), java(), javaDoc(),
                             checkStyle(),
                             spotBugs(),
                             pmdParser(pattern: '**/target/pmd.xml'),
                             cpd(pattern: '**/target/cpd.xml')
                         ]
        }

        // Send notification email when a build fails, has test failures, or is the first successful build
        failure {
            script {
                echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                if (params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                    echo 'Sending notify'
                    emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                    body: '${DEFAULT_CONTENT}'
                }
            }
        }

        unstable {
            script {
                echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                if (params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                    echo 'Sending notify'
                    emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                        body: '${DEFAULT_CONTENT}'
                }
            }
        }

        fixed {
            script {
                echo "Build result notify policy is: ${params.BUILD_RESULT_NOTIFY}"
                if (params.BUILD_RESULT_NOTIFY == 'EMAIL') {
                    echo 'Sending notify'
                    emailext to: '${DEFAULT_RECIPIENTS}', subject: '${DEFAULT_SUBJECT}',
                        body: '${DEFAULT_CONTENT}'
                }
            }
        }

        cleanup {
            // Archive logs and dump files
            sh 'find . \\( -name "*.log" -o -name "*.dump*" -o -name "hs_err_*" -o -name "*.hprof" \\) -exec xz -3 {} \\;'
            archiveArtifacts allowEmptyArchive: true, artifacts: '**/*.xz,**/*.log.gz,documentation/target/generated-html/**,**/target/*-reports*/**/TEST-*.xml'

            // Remove all untracked files, ignoring .gitignore
            sh 'git clean -qfdx || echo "git clean failed, exit code $?"'

            // Remove all created SNAPSHOT artifacts to ensure a clean build on every run
            sh 'find ~/.m2/repository -type d -name "*-SNAPSHOT" -prune -exec rm -rf {} \\;'
        }
    }
}
