package at.dobiasch.mapreduce;

import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;

import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.hubnet.client.LoginCallback;
import org.nlogo.hubnet.client.LoginDialog;
import org.nlogo.hubnet.protocol.Message;
import org.nlogo.workspace.AbstractWorkspace;

import org.nlogo.BasicClient;

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
	
	public Node(String user, String host, int port)
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
		login();
	}
	
	private void login()
	{
		System.out.println(user + "," + host + "," + port);
		this.client= BasicClient.create(this.user, "MAPREDUCE", this.host, this.port);
		if( dlg != null )
			dlg.setVisible(false);
		// this.hubnet= this.workspace.getHubNetManager();
		node();
	}
	
	private void node()
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
					if( msg.equalsIgnoreCase("Some(Text(CONFIG-START,TEXT))"))
					{
						boolean endReached= false;
						Map<String,String> fields= new HashMap<String,String>();
						while( !endReached )
						{
							// hubnet.fetchMessage();
							// if(hubnet.getMessageTag().equals("CONFIG-END"))
							if(msg.equalsIgnoreCase("Some(Text(CONFIG-END,TEXT))"))
								endReached= true;
							else
							{
								String key, value;
								// key= hubnet.getMessageTag();
								// value= hubnet.getMessageSource();
								// TODO: 
								// fields.put(key, value);
							}
						}
						FrameworkFactory.getInstance().getConfiguration().setValues(fields);
						System.out.println("config send end");
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
