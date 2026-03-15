plugins {
    java
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.hazardcast"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Database
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("com.h2database:h2") // for tests and local dev

    // HTTP client
    implementation("org.springframework.boot:spring-boot-starter-webflux")

    // Parquet export
    implementation("org.apache.parquet:parquet-avro:1.14.3")
    implementation("org.apache.hadoop:hadoop-common:3.4.1") {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
    }
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1") {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
    }

    // XGBoost inference
    implementation("ml.dmlc:xgboost4j_2.12:2.1.3")

    // JSON
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Lombok
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testRuntimeOnly("com.h2database:h2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
