import org.nlogo.headless.HeadlessWorkspace
object RunCommand {
  def main(args: Array[String]) {
    val workspace = HeadlessWorkspace.newInstance
    workspace.open(args(0))
    println("RunCommand: run")
    for( i <- 1 to args.length - 1) {
      workspace.command(args(i))
      Thread.sleep(1000)
    }
    println("RunCommand: done")
    workspace.dispose()

	sys.exit(0)
  }
}
