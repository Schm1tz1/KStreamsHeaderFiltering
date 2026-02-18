# Template KStreams Filter App

## Processing Logic
...

## Data Format / (De-)Serialization
...

## Build
- using maven: `mvn compile`
- packaging to jar files: `mvn package`

## Testing
- Unit test for components and TTD tests are included
- included in maven build, separate test run: `mvn test`

## Application Configuration
...

## Deployment, Running
- A fat jar file can be created in addition to avoid dependency issues that can be use for single-file-deployments (maven assembly plugin in pom.xml)

## Log Format
- Default configuration is built into the jar but can be overridden on the command line by passing a configuration file using `-Dlog4j.configuration`, example:
  ```bash
  java -jar target/MyApp.jar -Dlog4j.configuration=file:/path/to/log4jconfig.properties
  ```
- Logging is per default done on INFO level to STDOUT using slf4j-simple

## Metrics
- Per default no additional metrics are exposed.

