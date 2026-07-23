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
    // Plain Iceberg Java API - core table/catalog model and the generic Record data model.
    implementation(libs.bundles.iceberg)

    // Parquet support is only needed at *runtime*: reading/writing goes through Iceberg's
    // format registry (FormatModelRegistry / GenericFileWriterFactory, both in iceberg-core /
    // iceberg-data), which looks up iceberg-parquet's registered FormatModel dynamically. This
    // class deliberately never imports anything from org.apache.iceberg.parquet or
    // org.apache.parquet.* directly - see IcebergPartitionCompactor's class Javadoc.
    runtimeOnly(libs.iceberg.parquet)
    runtimeOnly(libs.iceberg.orc)


    // iceberg-parquet's writer/reader internals load org.apache.hadoop.conf.Configuration even
    // without a Hadoop catalog/FileIO in use, and iceberg-parquet does not pull it in
    // transitively (github.com/apache/iceberg/issues/10180). Runtime-only, and the lighter
    // hadoop-client-api/-runtime pair rather than the legacy hadoop-common, which drags in an
    // older, potentially conflicting transitive dependency tree (e.g. Jackson).
    runtimeOnly(libs.hadoop.client.api)
    runtimeOnly(libs.hadoop.client.runtime)

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