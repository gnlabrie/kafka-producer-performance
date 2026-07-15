package com.kafkaproducertest;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "com.kafkaproducertest")
class ArchitectureTest {
  @ArchTest
  static final ArchRule domainPackagesDoNotDependOnCli =
      noClasses()
          .that()
          .resideInAnyPackage("..config..", "..payload..", "..statistics..")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("..cli..");

  @ArchTest
  static final ArchRule configurationDoesNotDependOnExecutionOrKafka =
      classes()
          .that()
          .resideInAPackage("..config..")
          .should()
          .onlyDependOnClassesThat()
          .resideOutsideOfPackages("..execution..", "..kafka..", "..payload..", "..statistics..");
}
