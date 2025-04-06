# Metabase Arrow Flight SQL Driver

A Clojure library that enables Metabase to connect to databases using the Apache Arrow Flight SQL JDBC driver. This driver integrates Arrow Flight SQL into Metabase, delivering enhanced performance and advanced SQL querying capabilities.

## Features

- JDBC-based integration: Leverages the Apache Arrow Flight SQL JDBC driver.
- Flexible configuration: Supports custom connection properties such as host, port, user, password, token, and encryption.
- Custom Schema Sync: Implements table and column description methods for seamless Metabase integration.
- Timestamp Conversion: Automatically converts TIMESTAMP columns to local date-time objects.

## Installation

### Prerequisites

- Leiningen (https://leiningen.org/) – for building the project.
- Podman (https://podman.io/) – for container management.
- Metabase (https://www.metabase.com/) – the latest version is recommended.

### Building the Driver

1. Clone the Repository

   git clone https://github.com/J0hnG4lt/metabase-flightsql-driver.git
   cd metabase-driver

2. Build the Uberjar

   lein uberjar


The docker-compose file also contains a builder service, so don´t worry if you have issues when installing lein and clojure.

### Deploying with Podman Compose

Run the following command:

```
podman compose up --build
```

This launches the following services:

1. Builder: this uses lein to produce an uberjar that is stored in a volume.
2. Spice AI: this allows us to read a parquet and make it available through the Arrow Flight SQL protocol.
3. Postgres: this is the internal database used by Metabase.
4. Metabase: this is the BI tool for which we are building this driver. It reads the JAR from the builder services and it uses postgres to store its configuration, charts, dashboards, etc. 

## Configuration

When setting up the connection in Metabase, the driver registers under the name :arrow-flight-sql with :sql-jdbc as its parent. The main configuration properties include:

- Host: (Default: localhost) – The server's hostname or IP address.
- Port: (Default: 443) – The port to use for the connection.
- User: (Optional) – Username for authentication.
- Password: (Optional) – Password for authentication (supports secret merging).
- Token: (Optional) – A secure token for connection.
- Use Encryption: (Default: true) – Enable or disable connection encryption. We don´t use this in our tests.

Advanced options can be set via the additional-options field.

## Integration with Metabase

To integrate the driver into Metabase:

1. Place the JAR in the Plugins Directory  
   Ensure that the built uberjar (`metabase-flightsql-driver-0.1.0-SNAPSHOT-standalone.jar`) is located in the Metabase plugins directory and rename it to `flightsql-metabase-driver.jar`.

2. Update the Metabase Container  
   Modify your container settings (as shown above) to mount the driver JAR into the /plugins directory.

3. Restart Metabase  
   Restart the container to load the new driver.

## Testing the Connection

The driver includes a built-in connection test. If you encounter any issues:

- Verify your configuration details.
- Check the logs for any error messages (e.g., issues with the Flight SQL connection).

## Project Structure

A simplified overview of the project structure:

```
.
├── .calva
├── .clj-kondo
├── data
├── doc
├── node_modules
├── resources
├── src
│   └── metabase
│       └── driver
└── target
```
## License

Copyright © 2025 Georvic Tur

This project is available under the terms of the Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0), which is the most permissive license possible that is compatible with Flight SQL and Metabase. Additionally, the source code may be distributed under the terms of the Eclipse Public License 2.0 (http://www.eclipse.org/legal/epl-2.0) or the GNU General Public License (GPL) version 2 or later with the GNU Classpath Exception, subject to the conditions specified in the Eclipse Public License.

## AI-Assisted Development

This project was built with help from ChatGPT, along with reference to the Metabase repository and several of its existing drivers. While I'm not a Clojure developer by background, these tools made development much more approachable.

## Features that have been tested so far

- SQL Native queries
- Graphical query editor (manual changes of the metadata are needed at the moment)
- Database syncs

## Contributing

Contributions are welcome! If you have suggestions or improvements, please open an issue or submit a pull request.

## Contact

For additional information or support, please open an issue in the repository or contact the maintainer at [georvic.tur@gmail.com].
