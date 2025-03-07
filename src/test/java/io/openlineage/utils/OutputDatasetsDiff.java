/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.utils.Config.IgnoredFacets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Verify output dataset facets")
public class OutputDatasetsDiff {

  private static Context context;
  private static OutputDatasetHelper datasetHelper;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
    datasetHelper = new OutputDatasetHelper(context);
  }

  @Test
  @DisplayName("Verify output dataset names and namespaces")
  void verifyOutputNames() {
    Set<DatasetIdentifier> prevIdentifiers = context
        .getPrevEvents()
        .stream()
        .flatMap(e -> e.getOutputs().stream())
        .map(d -> new DatasetIdentifier(d.getNamespace(), d.getName()))
        .collect(Collectors.toSet());

    Set<DatasetIdentifier> nextIdentifiers = context
        .getNextEvents()
        .stream()
        .flatMap(e -> e.getOutputs().stream())
        .map(d -> new DatasetIdentifier(d.getNamespace(), d.getName()))
        .collect(Collectors.toSet());

    assertThat(nextIdentifiers)
        .describedAs("Prev and next should detect the same number of datasets")
        .hasSize(prevIdentifiers.size());
    assertThat(nextIdentifiers)
        .describedAs("Prev and next should have same datasets identified")
        .containsAll(prevIdentifiers);
  }

  @ParameterizedTest
  @MethodSource("prevDatasetFacets")
  @DisplayName("Verify dataset facet: {} {}")
  void verifyDatasetFacet(DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true)
          .describedAs("No output datasets found")
          .isTrue();
    }
    return;
  }

  @ParameterizedTest
  @MethodSource("prevOutputFacets")
  @DisplayName("Verify output dataset facets {}")
  void verifyOutputtDatasetFacet(DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true)
          .describedAs("No output datasets found")
          .isTrue();
      return;
    }
    OutputDatasetFacet prevFacet = datasetHelper
        .mergedOutputDatasetFacets(datasetHelper.prevOutputs(di))
        .get(facetName);

    OutputDatasetFacet nextFacet = datasetHelper
        .mergedOutputDatasetFacets(datasetHelper.nextOutputs(di))
        .get(facetName);

    assertThat(nextFacet)
        .overridingErrorMessage("Next facets should contain facet: " + facetName)
        .isNotNull();

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev input facet additional properties")
        .containsAllEntriesOf(prevFacet.getAdditionalProperties());
  }

  private static Stream<Arguments> prevOutputFacets() {
    List<Arguments> args =  datasetHelper
        .prevOutputFacets()
        .entrySet()
        .stream()
        .flatMap(entry ->
            entry
                .getValue()
                .stream()
                .filter(facet -> Optional
                    .ofNullable(context.getConfig().getIgnoredFacets())
                    .map(IgnoredFacets::getOutputDatasetFacets)
                    .filter(ignoredFacets -> ignoredFacets.contains(facet))
                    .isEmpty()
                )
                .map(facetName -> Arguments.of(
                    Named.of("Dataset " + entry.getKey().getName(), entry.getKey()),
                    Named.of("Facet " + facetName, facetName)
                ))
        )
        .toList();

    if (args.isEmpty()) {
      return Stream.of(Arguments.of(
          Named.of("No input facets to verify", null),
          null
      ));
    } else {
      return args.stream();
    }
  }

  private static Stream<Arguments> prevDatasetFacets() {
    List<Arguments> args =  datasetHelper
        .prevFacets()
        .entrySet()
        .stream()
        .flatMap(entry ->
            entry
                .getValue()
                .stream()
                .filter(facet -> Optional
                    .ofNullable(context.getConfig().getIgnoredFacets())
                    .map(IgnoredFacets::getDatasetFacets)
                    .filter(ignoredFacets -> ignoredFacets.contains(facet))
                    .isEmpty()
                )
                .map(facetName -> Arguments.of(
                    Named.of("Dataset " + entry.getKey().getName(), entry.getKey()),
                    Named.of("Facet " + facetName, facetName)
                ))
        )
        .toList();

    if (args.isEmpty()) {
      return Stream.of(Arguments.of(
          Named.of("No facets to verify", null),
          null
      ));
    } else {
      return args.stream();
    }
  }
}
