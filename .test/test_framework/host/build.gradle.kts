/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("java")
}

group = "software.amazon.go.integration.tests"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.checkerframework:checker-qual:3.26.0")
    testImplementation("org.junit.platform:junit-platform-commons:1.9.0")
    testImplementation("org.junit.platform:junit-platform-engine:1.9.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.9.0")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.9.0")
    testImplementation("org.postgresql:postgresql:42.5.0")
    testImplementation("mysql:mysql-connector-java:8.0.30")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("software.amazon.awssdk:rds:2.31.64")
    testImplementation("software.amazon.awssdk:ec2:2.31.64")
    testImplementation("software.amazon.awssdk:secretsmanager:2.31.64")
    testImplementation("org.testcontainers:testcontainers:1.17.4")
    testImplementation("org.testcontainers:postgresql:1.17.5")
    testImplementation("org.testcontainers:mysql:1.17.+")
    testImplementation("org.testcontainers:junit-jupiter:1.17.4")
    testImplementation("org.testcontainers:toxiproxy:1.20.3")
    testImplementation("org.apache.poi:poi-ooxml:5.2.2")
    testImplementation("org.slf4j:slf4j-simple:2.0.3")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.14.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.29.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.29.0")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:1.29.0")
}

tasks.test {
    filter.excludeTestsMatching("integration.*")
}

tasks.withType<Test> {
    useJUnitPlatform()
    outputs.upToDateWhen { false }
    testLogging {
        events(PASSED, FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    reports.junitXml.required.set(true)
    reports.html.required.set(false)
}

tasks.register<Test>("test-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-performance", "true")
    }
}

tasks.register<Test>("test-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}


tasks.register<Test>("test-aurora-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-all-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
    }
}

tasks.register<Test>("test-aurora-pg-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-aurora-mysql-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}


tasks.register<Test>("test-multi-az-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-multi-az-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-autoscaling") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-autoscaling-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-autoscaling-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-bgd-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-bgd-mysql-multiaz") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "false")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-bgd-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-bgd-pg-multiaz") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "false")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("test-aurora-limitless-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-multi-az", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "false")
        systemProperty("limitless-only", "true")
        systemProperty("exclude-race", "true")
    }
}

// TODO: change these filters
tasks.register<Test>("test-aurora-pg-race") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-multi-az", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "false")
    }
}

tasks.register<Test>("test-aurora-mysql-race") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-multi-az", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "false")
    }
}

// Debug

tasks.register<Test>("debug-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora-pg-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora-mysql-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-multi-az-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-multi-az-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-autoscaling") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("test-autoscaling", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-aurora-limitless-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-failover", "true")
        systemProperty("exclude-multi-az", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-limitless", "false")
        systemProperty("limitless-only", "true")
        systemProperty("exclude-race", "true")
    }
}

tasks.register<Test>("debug-bgd-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-limitless", "true")
        systemProperty("exclude-race", "true")
    }
}
