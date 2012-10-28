import org.nlogo.headless.HeadlessWorkspace
object RunCommand {
  def main(args: Array[String]) {
    val workspace = HeadlessWorkspace.newInstance
    workspace.open(args(0))
	println("RunCommand: run")
    workspace.command(args(1))
	    workspace.dispose()
	println("RunCommand: done")
	sys.exit(0)
  }
}
