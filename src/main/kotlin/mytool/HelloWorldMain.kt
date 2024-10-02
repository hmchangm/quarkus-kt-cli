package mytool

import io.agroal.api.AgroalDataSource
import io.quarkus.logging.Log
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBDriver
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

@QuarkusMain
class HelloWorldMain(
    private val datasource: AgroalDataSource,
    private val dataImporter: DataImporter,
) : QuarkusApplication {
    override fun run(vararg args: String): Int {
        val arg = if (args.isNotEmpty()) args[0] else ""
        Log.info("Hello $arg")
        if (arg == "import") {
            dataImporter.dummyInsert()
            return 0
        }

        val props = Properties().apply { setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, true.toString()) }
        val conn = DriverManager.getConnection("jdbc:duckdb:$arg", props) as DuckDBConnection

        val pgConn = datasource.connection

// create a table
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
        CREATE TABLE IF NOT EXISTS addresses (id INTEGER PRIMARY KEY,
            street_address VARCHAR(100),city VARCHAR(50),
            state VARCHAR(50),postal_code VARCHAR(20),country VARCHAR(50))""",
            )
        }

        dataLoadToDuck(
            pgConn,
            "select id,street_address,city,state,postal_code,country from addresses",
            "addresses",
            conn,
        )
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
        CREATE TABLE IF NOT EXISTS address_ss (id INTEGER PRIMARY KEY,
            street_address VARCHAR(100),city VARCHAR(50),
            state VARCHAR(50),postal_code VARCHAR(20),country VARCHAR(50))""",
            )
        }

        dataLoadToDuck(
            pgConn,
            "select id,street_address,city,state,postal_code,country from addresses where id >40",
            "address_ss",
            conn,
        )

        conn.createStatement().use { stmt ->
            stmt
                .executeQuery(
                    "select * from addresses " +
                        " where id NOT IN (select id from address_ss) ",
                ).use { rs ->
                    while (rs.next()) {
                        Log.info("find ${rs.getInt(1)} ${rs.getString(2)}")
                    }
                }
        }
        Log.info("Finish load")

        return 0
    }

    private fun dataLoadToDuck(
        pgConn: Connection,
        sql: String,
        targetTbl: String,
        conn: DuckDBConnection,
    ): AtomicInteger {
        val cnt = AtomicInteger(0)
        // insert two items into the table
        Log.info("Start to load $targetTbl")
        conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, targetTbl).use { appender ->
            pgConn.createStatement().use { statement ->
                statement
                    .executeQuery(sql)
                    .use { rs ->
                        while (rs.next()) {
                            appender.beginRow()
                            appender.append(rs.getInt(1))
                            appender.append(rs.getString(2))
                            appender.append(rs.getString(3))
                            appender.append(rs.getString(4))
                            appender.append(rs.getString(5))
                            appender.append(rs.getString(6))
                            appender.endRow()
                            if ((cnt.getAndAdd(1) % 100000) == 0) Log.info("$targetTbl $cnt")
                        }
                    }
            }
        }
        Log.info("All $targetTbl $cnt")
        return cnt
    }
}
