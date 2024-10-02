package mytool

import io.quarkus.logging.Log
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBDriver
import java.sql.DriverManager
import java.util.Date
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


@QuarkusMain
class HelloWorldMain : QuarkusApplication {

    override fun run(vararg args: String): Int {
        val arg = if (args.isNotEmpty()) args[0] else ""
        Log.info ("Hello $arg")
        if(arg == "import") {
            dummyInsert()
            return 0
        }

        val props = Properties().apply{ setProperty(DuckDBDriver.JDBC_STREAM_RESULTS,true.toString())}
        val conn = DriverManager.getConnection("jdbc:duckdb:$arg",props) as DuckDBConnection

        val url = "jdbc:postgresql://localhost:5432/iron"
        val user = "iron"
        val password = "mySecret"

        val pgConn = DriverManager.getConnection(url, user, password)

// create a table
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
        CREATE TABLE IF NOT EXISTS addresses (id INTEGER PRIMARY KEY,
            street_address VARCHAR(100),city VARCHAR(50),
            state VARCHAR(50),postal_code VARCHAR(20),country VARCHAR(50))"""
            )
        }
        val cnt = AtomicInteger(0)
// insert two items into the table
        Log.info("Start to load addresses")
        conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "addresses").use { appender ->
            pgConn.createStatement().use { statement ->
                statement.executeQuery("select id,street_address,city,state,postal_code,country from addresses")
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
                            if ((cnt.getAndAdd(1) % 100000) == 0) Log.info("$cnt")
                        }
                    }
            }
        }
        Log.info("All addresses $cnt")
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
        CREATE TABLE IF NOT EXISTS address_ss (id INTEGER PRIMARY KEY,
            street_address VARCHAR(100),city VARCHAR(50),
            state VARCHAR(50),postal_code VARCHAR(20),country VARCHAR(50))"""
            )
        }
        cnt.getAndSet(0)
// insert two items into the table
        Log.info("Start to load address_ss")
        conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "address_ss").use { appender ->
            pgConn.createStatement().use { statement ->
                statement.executeQuery("select id,street_address,city,state,postal_code,country from addresses where id > 40")
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
                            if ((cnt.getAndAdd(1) % 100000) == 0) Log.info("$cnt")
                        }
                    }
            }
        }
        println("${Date()} all address_es $cnt")


        conn.createStatement().use { stmt ->
            stmt.executeQuery("select * from addresses " +
                    " where id NOT IN (select id from address_ss) ").use { rs ->
                while(rs.next()) {
                    Log.info("find ${rs.getInt(1)} ${rs.getString(2)}")
                }
            }
        }
        Log.info("Finish load")

        return 0
    }
}