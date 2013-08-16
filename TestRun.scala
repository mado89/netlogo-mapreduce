import java.awt.EventQueue
import org.nlogo.app.App
import org.nlogo.lite.InterfaceComponent
import org.nlogo.headless.HeadlessWorkspace

object TestRun
{
  def main(args: Array[String])
  {
    val workspace = HeadlessWorkspace.newInstance
    workspace.open("/home/martin/DA/mapreduce/WC-Distributed.nlogo")
    workspace.command("server")
    val server= new Thread(new Runnable {
      def run()
      {
        //Thread.sleep(100)
	//workspace.command("server")
	Thread.sleep(3000)
	println("Beginning MR")
	workspace.command("wc")
      }
    })
        
    val workspace2 = HeadlessWorkspace.newInstance
    workspace2.open("/home/martin/DA/mapreduce/WC-Distributed.nlogo")
    server.start
    workspace2.command("client")
    // workspace1.command("mapreduce:test")
    // wait { 
    //   workspace1.command("print \"test\"")
    //  workspace1.command("mapreduce:test") }
    // workspace.dispose()
    /*println(args)
    val frame1= new javax.swing.JFrame
    val comp1 = new InterfaceComponent(frame1)
    val frame2= new javax.swing.JFrame
    val comp2 = new InterfaceComponent(frame2)
    wait {
      frame1.setSize(1000,700)
      frame1.add(comp1)
      frame1.setVisible(true)
      comp1.open("/home/martin/DA/mapreduce/test.nlogo")
    }
    wait {
      frame2.setSize(1000,700)
      frame2.add(comp2)
      frame2.setVisible(true)
      comp2.open("/home/martin/DA/mapreduce/test.nlogo")
    }*/
    // App.app.command("set density 62")
    // println(App.app.report("burned-trees"))
  }
  def wait(block: => Unit) {
    java.awt.EventQueue.invokeAndWait(
      new Runnable() { def run() { block } } ) }
}

