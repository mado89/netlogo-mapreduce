package at.dobiasch.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;

import org.nlogo.BasicClient;
import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.hubnet.client.LoginCallback;
import org.nlogo.hubnet.client.LoginDialog;
import org.nlogo.hubnet.protocol.Message;

import scala.Option;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;

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
	private String world;
	private String modelpath;
	private int port;
	
	// The Controller for all Computing related tasks
	private HostController controller;
	private Map<Integer,String> inpids;
	
	public Node(String world, String modelpath)
	{
		this.world= world;
		this.modelpath= modelpath;
		
		inpids= new HashMap<Integer,String>();
		startIt();
	}
	
	public Node(String user, String host, int port, String world, String modelpath) throws ExtensionException
	{
		this.user= user;
		this.host= host;
		this.port= port;
		this.world= world;
		this.modelpath= modelpath;
		
		inpids= new HashMap<Integer,String>();
		
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
		System.out.println(" -> connected");
		if( dlg != null )
			dlg.setVisible(false);
		// this.hubnet= this.workspace.getHubNetManager();
		try{
			node();
		} catch( Exception e ) {
			System.out.println("Exception in node(): " + e.getMessage() );
			e.printStackTrace();
		}
	}
	
	private void node() throws ExtensionException
	{
		boolean run= true;
		while(run)
		{
			System.out.println("running");
			try
			{
				Option<Message> message;
				Option<Message> None= scala.Option.apply(null);
				String msg;
				// if( hubnet.messageWaiting() )
				message= this.client.nextMessage(0); // scala.Option.apply(null);
				if( message != null && message != None )
				{
					// hubnet.fetchMessage();
					// System.out.println("Message: " + message );
					msg= message.toString();
					//if( hubnet.getMessageTag().equals("CONFIG-START"))
					if( msg.startsWith("Some(Text(CONFIG:"))
					{
						FrameworkFactory.getInstance().getConfiguration().
							setValuesFromString(msg.substring(19, msg.length() - 7));
						System.out.println("Config recieved: " + 
							FrameworkFactory.getInstance().getConfiguration());
						setup();
					} else if (msg.startsWith("Some(Text(MAP:")) {
						mapAssignment(msg.substring(14, msg.length() - 7));
					} else if (msg.startsWith("Some(Text(INPIDS:")) {
						inputIDs(msg.substring(18, msg.length() - 8));
					} else if (msg.startsWith("Some(Text(ASSIGNED")) {
						try {
							finishMapStage();
						} catch (ExtensionException e) {
							this.client.sendActivityCommand("failed-node", this.user);
						}
						try {
							doReduce();
							doCollect();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (CompilerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						System.out.println("Message: " + message );
					}
				}
				// else
				// 	System.out.println("Node:null/None");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.out.println("Node interruped");
				run= false;
				e.printStackTrace();
			}/* catch (LogoException e) {
				run= false;
				e.printStackTrace();
			}*/
		}
	}
	
	public void setup() throws ExtensionException
	{
		System.out.println("Setting up");
		// TODO: its assumed that working/system directory is created and write able
		// TODO: check if config was received before
		
		Framework fw= FrameworkFactory.getInstance();
		this.controller= new HostController( fw.getConfiguration().getMappers(),
				fw.getConfiguration().getReducers(),
				fw.getConfiguration().getMapper(), 
				fw.getConfiguration().getReducer(),
				fw.getSystemFileHandler(), 
				this.world, this.modelpath);
		
		fw.setHostController(this.controller);
		
		try
		{
			this.controller.prepareMappingStage();
		} catch (CompilerException e){
			throw new ExtensionException(e);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}

	private void inputIDs(String string) {
		System.out.println(string);
		String[] vals= string.split(",");
		
		int i;
		for(i= 0; i < vals.length; i++)
		{
			// System.out.println(vals[i]);
			String[] h= vals[i].split(":");
			this.inpids.put(Integer.parseInt(h[1]), h[0]);
		}
	}

	private void mapAssignment(String assignment) {
		// System.out.println(assignment);
		
		String[] vals= assignment.split(",");
		
		int i;
		for(i= 0; i < vals.length - 1; i++)
		{
			// System.out.println(vals[i]);
			
			if( !vals[i].equals("") )
			{
				String[] h= vals[i].split("-");
				
				// h[0] node name
				// h[1] tID
				// h[2] inpid
				// h[3] partStart
				// h[4] partEnd
				if( h[0].equals(this.user) )
				{
					// System.out.println("Controller: " + this.controller);
					// System.out.println("Inpid: " + this.inpids + " : " + h[2]);
					this.controller.addMap(Long.parseLong(h[1]), this.inpids.get(Integer.parseInt(h[2])), 
							Long.parseLong(h[3]), Long.parseLong(h[4]));
				}
			}
		}
	}
	
	private void finishMapStage() throws ExtensionException {
		boolean result= this.controller.waitForMappingStage();
		if( result == false)
			throw new ExtensionException("Mapping-Stage failed");
		
		this.controller.finishMappingStage();
		
		System.out.println("done mapping");
	}
	
	/**
	 * Start the Reducing Stage
	 * Wait for the end
	 * @throws IOException
	 * @throws CompilerException
	 * @throws ExtensionException
	 */
	private void doReduce() throws IOException, CompilerException, ExtensionException
	{
		Map<String,IntKeyVal> intdata= this.controller.getIntermediateData();
		String[] kk= new String[intdata.size()];
		intdata.keySet().toArray(kk);
		
		System.out.println(this.user + ": Begin Reducing");
		this.controller.prepareReduceStage();
		
		for(int i= 0; i < kk.length; i++)
		{
			this.controller.addReduce(this.controller.getID(), kk[i], intdata.get(kk[i]));
		}
				
		boolean result= this.controller.waitForReduceStage();
		if( result == false)
			throw new ExtensionException("Reduce-Stage failed");
		
		this.controller.finishReduceStage();
		System.out.println("Reducing ended");
	}

	/**
	 * Collect the results of the reducers
	 * Send it to the master
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void doCollect() throws IOException, InterruptedException
	{
		System.out.println("Writing output");
		
		String results= this.controller.mergeReduceOutputD();
		
		// System.out.println("Node:" + results);
		this.client.sendActivityCommand("results", results);
	}
}
