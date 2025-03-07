/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/


package io.openlineage.utils;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Common methods to be shared among the tests
 */
@Getter
@AllArgsConstructor
public class Context {

  List<RunEvent> prevEvents;
  List<RunEvent> nextEvents;

  public static Context loadContext() {
    return new Context(
        getRunEvents(System.getProperty("prev.path")),
        getRunEvents(System.getProperty("next.path"))
    );
  }

  @SneakyThrows
  public static List<RunEvent> getRunEvents(String path) {
    return Files
        .lines(Path.of(path))
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }
}
