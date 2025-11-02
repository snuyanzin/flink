package org.apache.flink.table.planner;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test to check whether there is a plan without any corresponding test. */
class AbandonedPlanTest {
    @Test
    void testIfThereAreAbandonedPlans() {
        final Path resources = Paths.get("src", "test", "resources");
        final String packageName =
                AbandonedPlanTest.class.getPackageName().replace(".", File.separator);
        final Path resourcePath = Paths.get(resources.toString(), packageName);
        final Set<String> plans = new HashSet<>();
        walk(
                resourcePath,
                path -> {
                    if (path.toString().endsWith("Test.xml")) {
                        plans.add(
                                path.toString()
                                        .replace(resources.toString(), "")
                                        .replace(".xml", ""));
                    }
                });

        final Path javaTestPath = Paths.get("src", "test", "java");
        final Path scalaTestPath = Paths.get("src", "test", "scala");
        final Path pathForJavaClasses = Paths.get(javaTestPath.toString(), packageName);
        walk(
                pathForJavaClasses,
                path -> {
                    if (path.toString().endsWith("Test.java")) {
                        plans.remove(
                                path.toString()
                                        .replace(javaTestPath.toString(), "")
                                        .replace(".java", ""));
                    }
                });

        final Path pathForScalaClasses = Paths.get(scalaTestPath.toString(), packageName);
        walk(
                pathForScalaClasses,
                path -> {
                    if (path.toString().endsWith("Test.scala")) {
                        plans.remove(
                                path.toString()
                                        .replace(scalaTestPath.toString(), "")
                                        .replace(".scala", ""));
                    }
                });

        assertThat(plans).as("There xml plans without corresponding java or scala tests").isEmpty();
    }

    private void walk(Path startPath, Consumer<Path> consumer) {
        try {
            Files.walkFileTree(
                    startPath,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            consumer.accept(file);
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
