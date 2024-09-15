package mytool

import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain


@QuarkusMain
class HelloWorldMain : QuarkusApplication {

    override fun run(vararg args: String): Int {
        val arg = if(args.isNotEmpty()) args[0] else "Hello World"
        println("Hello $arg")
        return 0
    }
}