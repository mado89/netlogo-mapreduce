import java.awt.EventQueue
import org.nlogo.app.App
import org.nlogo.lite.InterfaceComponent
import org.nlogo.headless.HeadlessWorkspace

object TestSingleNode
{
  def main(args: Array[String])
  {
    val workspace = HeadlessWorkspace.newInstance
    workspace.open("/home/martin/DA/mapreduce/test.nlogo")
    workspace.command("mapreduce:mapreduce")
  }
  def wait(block: => Unit) {
    java.awt.EventQueue.invokeAndWait(
      new Runnable() { def run() { block } } ) }
}

