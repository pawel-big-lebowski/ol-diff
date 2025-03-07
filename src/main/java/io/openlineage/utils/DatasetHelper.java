/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static io.openlineage.client.OpenLineageClientUtils.newObjectMapper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatasetHelper {

  Context context;
  ObjectMapper mapper = newObjectMapper();

  // TODO: this is work in progress

  public Map<String, DatasetFacet> mergedDatasetFacets(List<Dataset> datasetList) {
    return datasetList
        .stream()
        .map(d -> mapper.convertValue(d.getFacets(), new TypeReference<Map<String, DatasetFacet>>() {}))
        .reduce(((map1, map2) -> {
          map1.putAll(map2);
          return map1;
        }))
        .orElse(Collections.EMPTY_MAP);
  }

  public Map<String, InputDatasetFacet> mergedInputDatasetFacets(List<InputDataset> datasetList) {
    return datasetList
        .stream()
        .map(d -> mapper.convertValue(d.getInputFacets(), new TypeReference<Map<String, InputDatasetFacet>>() {}))
        .reduce(((map1, map2) -> {
          map1.putAll(map2);
          return map1;
        }))
        .orElse(Collections.EMPTY_MAP);
  }

  public Map<String, OutputDatasetFacet> mergedOutputDatasetFacets(List<OutputDataset> datasetList) {
    return datasetList
        .stream()
        .map(d -> mapper.convertValue(d.getOutputFacets(), new TypeReference<Map<String, OutputDatasetFacet>>() {}))
        .reduce(((map1, map2) -> {
          map1.putAll(map2);
          return map1;
        }))
        .orElse(Collections.EMPTY_MAP);
  }
}
