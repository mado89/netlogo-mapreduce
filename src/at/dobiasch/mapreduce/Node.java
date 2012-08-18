package at.dobiasch.mapreduce;

import javax.swing.JFrame;

import org.nlogo.hubnet.client.LoginCallback;
import org.nlogo.hubnet.client.LoginDialog;

import org.nlogo.BasicClient;

/**
 * This is the main class for the Node
 * By calling startIt the HubNet Login-Window is invoked
 * @author Martin Dobiasch
 */
public class Node
{
	LoginDialog dlg;
	
	public Node()
	{
		startIt();
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
	        		login(user, host, port);
	        	}
			}
		);
	}
	
	private void login(String user, String host, int port)
	{
		BasicClient.create(user, "MAPREDUCE", host, port);
		dlg.setVisible(false);
	}
}
