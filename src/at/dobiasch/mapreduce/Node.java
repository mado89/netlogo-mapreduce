package at.dobiasch.mapreduce;

import javax.swing.JFrame;

import org.nlogo.BasicClient;
import org.nlogo.api.ExtensionException;
import org.nlogo.hubnet.client.LoginCallback;
import org.nlogo.hubnet.client.LoginDialog;
import org.nlogo.hubnet.protocol.Message;

import scala.Option;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * This is the main class for the Node
 * By calling startIt the HubNet Login-Window is invoked
 * @author Martin Dobiasch
 */
public class Node
{
	LoginDialog dlg;
	private BasicClient client;
	// private HubNetInterface hubnet;
	private String user;
	private String host;
	private int port;
	
	public Node()
	{
		startIt();
	}
	
	public Node(String user, String host, int port) throws ExtensionException
	{
		this.user= user;
		this.host= host;
		this.port= port;
		login();
	}
	
	public void startIt()
	{
		JFrame frame= new JFrame("HubNet");
		String userid = null, hostip = null;
		int port = 0;
		
		dlg= new LoginDialog(frame,userid, hostip, port, false);
		dlg.go(new LoginCallback()
			{
	        	public void apply(String user, String host, int port)
	        	{
	        		hlogin(user, host, port);
	        	}
			}
		);
	}
	
	private void hlogin(String user, String host, int port)
	{
		this.user= user;
		this.host= host;
		this.port= port;
		try {
			login();
		} catch (ExtensionException e) {
			e.printStackTrace();
		}
	}
	
	private void login() throws ExtensionException
	{
		System.out.println(user + "," + host + "," + port);
		this.client= BasicClient.create(this.user, "MAPREDUCE", this.host, this.port);
		if( dlg != null )
			dlg.setVisible(false);
		// this.hubnet= this.workspace.getHubNetManager();
		node();
	}
	
	private void node() throws ExtensionException
	{
		boolean run= true;
		while(run)
		{
			try
			{
				Option<Message> message;
				String msg;
				// if( hubnet.messageWaiting() )
				if( (message= this.client.nextMessage(0)) != null )
				{
					// hubnet.fetchMessage();
					System.out.println("Message: " + message);
					msg= message.toString();
					//if( hubnet.getMessageTag().equals("CONFIG-START"))
					if( msg.startsWith("Some(Text(CONFIG:"))
					{
						FrameworkFactory.getInstance().getConfiguration().
							setValuesFromString(msg.substring(19, msg.length() - 7));
						System.out.println("Config recieved: " + 
							FrameworkFactory.getInstance().getConfiguration());
					}
				}
				Thread.sleep(100);
			} catch (InterruptedException e) {
				run= false;
				e.printStackTrace();
			}/* catch (LogoException e) {
				run= false;
				e.printStackTrace();
			}*/
		}
	}
}
