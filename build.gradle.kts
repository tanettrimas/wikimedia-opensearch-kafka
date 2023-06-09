plugins {
    kotlin("jvm") version "1.8.21"
}

allprojects {
    group = "no.tanettrimas"
    version = "1.0-SNAPSHOT"
    apply(plugin = "org.jetbrains.kotlin.jvm")

    repositories {
        mavenCentral()
    }

    dependencies {
        testImplementation(kotlin("test"))
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")
    }

    tasks {
        test {
            useJUnitPlatform()
        }
        jar {
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }
    }

    kotlin {
        jvmToolchain(17)
    }
}

