
`ol-diff` is a tool to compare lineage events from different producers versions producing OpenLineage events for the same job.
It makes sure that events produced by a previous version are reflected in the next version. 
This can be useful when upgrading OpenLineage producer with custom changes implemented. 

`ol-diff` takes two sets of OpenLineage events, compares them and outputs the result into a report. 

Simple example usage:
```
./ol-diff.sh
```

TODO: 
 * Support extracting events from logs when ConsoleTransport enabled.
 * Implement diff for datasets.
 * Write documentation about what is checked.

## Development

As a developer you can run tests with an extra param to include internal tests - tests that test the tool itself. 

```
docker run --rm -u gradle -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:jdk17-ubi  gradle clean test -Pprev.path=examples/success/prev.txt -Pnext.path=examples/success/next.txt -Pinternal.tests=true
open build/reports/tests/test/index.html
```

Tests should then include extra classes like `JobHelpersTest` or `RunHelperTest`.