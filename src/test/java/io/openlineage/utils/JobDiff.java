/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.utils.Config.IgnoredFacets;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Verify job facets")
public class JobDiff {

  private static Context context;
  private static JobHelper jobHelper;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
    jobHelper = new JobHelper(context);
  }

  @DisplayName("Verifies identical job name")
  @Test
  void verifyJobNames() {
    assertThat(new HashSet<>(jobHelper.prevNames()))
        .describedAs("Set of job names from prev events. Should contain a single job name")
        .hasSize(1);

    assertThat(new HashSet<>(jobHelper.nextNames()))
        .describedAs("Set of job names from next events. Should contain a single job name")
        .hasSize(1);

    assertThat(jobHelper.prevNames().get(0))
        .describedAs("Prev job name")
        .isEqualTo(jobHelper.nextNames().get(0));
  }

  @DisplayName("Verifies identical job namespace")
  @Test
  void verifyJobNamespace() {
    assertThat(new HashSet<>(jobHelper.prevNamespaces()))
        .describedAs("Set of job namespaces from prev events. Should contain a single job namespace")
        .hasSize(1);

    assertThat(new HashSet<>(jobHelper.nextNamespaces()))
        .describedAs("Set of job namespaces from next events. Should contain a single job namespace")
        .hasSize(1);

    assertThat(jobHelper.prevNamespaces().get(0))
        .describedAs("Prev job name")
        .isEqualTo(jobHelper.nextNamespaces().get(0));
  }

  @ParameterizedTest
  @MethodSource("prevJobFacets")
  @DisplayName("Verify job facet {}")
  void verifyJobFacets(String prevFacetName, JobFacet prevFacet, JobFacet nextFacet) {
    assertThat(nextFacet)
        .overridingErrorMessage("Next job facets should contain prev job prevFacet: " + prevFacetName)
        .isNotNull();

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev job facet additional properties")
        .containsAllEntriesOf(prevFacet.getAdditionalProperties());
  }
  private static Stream<Arguments> prevJobFacets() {
    return jobHelper
        .prevMergedFacets()
        .entrySet()
        .stream()
        .filter(e -> Optional
            .ofNullable(context.getConfig().getIgnoredFacets())
            .map(IgnoredFacets::getJobFacets)
            .filter(ignoredFacets -> ignoredFacets.contains(e.getKey()))
            .isEmpty()
        )
        .map(e -> Arguments.of(
            Named.of("Compare job facet=" + e.getKey(), e.getKey()),
            Named.of("prev facet", e.getValue()),
            Named.of("with next", jobHelper.nextMergedFacets().getOrDefault(e.getKey(), null))
        ));
  }
}
