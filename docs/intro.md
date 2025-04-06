# Introduction to metabase-flightsql-driver

Run this command:

```bash
podman compose up --build
```

## Connection

If you have run the docker-compose file, a service that relies on [Spice AI](https://spiceai.org/docs/api/arrow-flight-sql) should be available.

When Metabase is finally up, you can add a connection to that service

![image](/docs/connection.png)

The next time you run this, you don´t need to build the everything from scratch:

```bash
podman compose up
```

After this, given that you have created a database and its configuration should be stored, you should see a log like this:

![image](/docs/healthcheck.png)


## Database sync

To speed up the process and ensure that you can see all tables on Spice AI, run a database sync

![image](/docs/database-sync.png)

## Run a query to test

![image](/docs/sql-editor.png)

## See the available schemas

![image](/docs/browse-data.png)

## Preview a table

![image](/docs/table.png)

## Use the visual query editor

![image](/docs/visual-editor.png)
