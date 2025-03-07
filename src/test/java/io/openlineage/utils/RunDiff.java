/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunFacet;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RunDiff {

  private static Context context;
  private static RunHelper runHelper;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
    runHelper = new RunHelper(context);
  }

  @ParameterizedTest
  @MethodSource("prevRunFacets")
  @DisplayName("Verify job facet {}")
  void verifyJobFacets(String prevFacetName, RunFacet prevFacet, RunFacet nextFacet) {
    assertThat(nextFacet)
        .overridingErrorMessage("Next run facets should contain prev run prevFacet: " + prevFacetName)
        .isNotNull();

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev run facet additional properties")
        .containsAllEntriesOf(prevFacet.getAdditionalProperties());
  }

  private static Stream<Arguments> prevRunFacets() {
    return runHelper
        .prevMergedFacets()
        .entrySet()
        .stream()
        .map(e -> Arguments.of(
            Named.of("Compare run facet=" + e.getKey(), e.getKey()),
            Named.of("with prev run facet", e.getValue()),
            Named.of("and next run facet", runHelper.nextMergedFacets().getOrDefault(e.getKey(), null))
        ));
  }
}
