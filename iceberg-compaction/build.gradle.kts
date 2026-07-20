plugins {
    `java-library`
}

group = "com.example.iceberg"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    // Plain Iceberg Java API - core table/catalog model, generic Record data model, Parquet I/O.
    implementation(libs.bundles.iceberg)

    // Logging: slf4j API at compile time, logback as the runtime backend.
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)

    // Tests: JUnit 5 (Jupiter) only - no AssertJ, no Spark.
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    // Iceberg's InMemoryCatalog + Parquet read/write for these tests is small, but give the
    // JVM a little breathing room for Parquet's column-chunk buffers.
    maxHeapSize = "1g"
}
