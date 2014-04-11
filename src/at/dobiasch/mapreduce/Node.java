package at.dobiasch.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;

import org.nlogo.BasicClient;
import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.hubnet.client.LoginCallback;
import org.nlogo.hubnet.client.LoginDialog;
import org.nlogo.hubnet.protocol.Message;

import ch.randelshofer.quaqua.ext.base64.Base64;

import scala.Option;
import at.dobiasch.mapreduce.framework.ChecksumHelper;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.partition.CheckPartData;
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
	// private String world;
	private String modelpath;
	private int port;
	
	// The Controller for all Computing related tasks
	private HostController controller;
	private Map<Integer,String> inpids;
	
	public Node(String modelpath)
	{
		// this.world= world;
		this.modelpath= modelpath;
		
		inpids= new HashMap<Integer,String>();
		startIt();
	}
	
	public Node(String user, String host, int port, /*String world, */String modelpath) throws ExtensionException
	{
		this.user= user;
		this.host= host;
		this.port= port;
		// this.world= world;
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
			throw new ExtensionException(e);
		}
	}
	
	private void node() throws ExtensionException
	{
		boolean run= true;
		boolean error= false;
		String reason= "";
		while(run)
		{
			// System.out.println("running");
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
					} else if (msg.startsWith("Some(Text(WORLD:")) {
						worldRecieved(msg.substring(16, msg.length() - 7));
					} else if (msg.startsWith("Some(Text(f-")) {
						filePartRecieved(msg.substring(12, msg.length() - 7));
					} else if (msg.startsWith("Some(Text(ASSIGNED")) {
						try {
							finishMapStage();
						} catch (ExtensionException e) {
							e.printStackTrace();
							System.out.println("Failure shutting down");
							this.client.sendActivityCommand("failed-node", this.user);
							
							error= true;
							reason= "Failure during Map-Stage occured";
							
							// By not setting run to false we can still recieve files
							//   -> missing files most likely have caused the error
							// run= false;
							
						}
						try {
							doReduce();
							doCollect();
						} catch (IOException e) {
							e.printStackTrace();
							this.client.sendActivityCommand("failed-node", this.user);
							throw new ExtensionException(e);
						} catch (CompilerException e) {
							e.printStackTrace();
							this.client.sendActivityCommand("failed-node", this.user);
							throw new ExtensionException(e);
						}
					} else if (msg.equals("Some(ExitMessage(Shutting Down.))")) {
						System.out.println("Shutdown signal recieved");
						run= false;
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
				this.client.sendActivityCommand("failed-node", this.user);
			}/* catch (LogoException e) {
				run= false;
				e.printStackTrace();
			}*/
		}
		
		if( error ) throw new ExtensionException(reason);
	}
	
	private void filePartRecieved(String msg) {
		int i= 0;
		while( msg.charAt(i) != '-') i++;
		int inpId= Integer.parseInt(msg.substring(0,i));
		int ss= ++i;
		while( msg.charAt(i) != '-') i++;
		int offs= Integer.parseInt(msg.substring(ss,i));
		byte[] decoded = Base64.decode(msg.substring(i+1));
		
		System.out.println("File part recieved " + inpId + " " + offs);
		
		RandomAccessFile out;
		try {
			out = new RandomAccessFile(this.inpids.get(inpId), "rw");
			out.seek(offs * 1024);
			out.write(decoded);
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setup() throws ExtensionException
	{
		System.out.println("Setting up");
		// TODO: its assumed that working/system directory is created and write able
		// TODO: check if config was received before
		
		Framework fw= FrameworkFactory.getInstance();
		
		// Create input-directory if it doesn't exist
		File f= new File(fw.getConfiguration().getInputDirectory());
		if( !f.exists() )
			f.mkdirs();
		
		this.controller= new HostController( fw.getConfiguration().getMappers(),
				fw.getConfiguration().getReducers(),
				fw.getConfiguration().getMapper(), 
				fw.getConfiguration().getReducer(),
				fw.getSystemFileHandler(), 
				this.modelpath);
		
		fw.setHostController(this.controller);
		
		/*try {
			prepareInput();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}*/
	}
	
	/**
	 * Once the world is recieved the Mapping-Stage can be prepared
	 * @param msg
	 * @throws ExtensionException
	 */
	private void worldRecieved(String msg) throws ExtensionException {
		try
		{
			byte[] decoded = Base64.decode(msg);
        	String world= null;
			try {
				world = new String(decoded, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			this.controller.prepareMappingStage(world);
		} catch (CompilerException e){
			throw new ExtensionException(e);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}

	private void inputIDs(String string) throws ExtensionException {
		System.out.println(string);
		String[] vals= string.split(",");
		
		int i;
		for(i= 0; i < vals.length; i++)
		{
			// System.out.println(vals[i]);
			String[] h= vals[i].split(":");
			int id= Integer.parseInt(h[1]);
			this.inpids.put(id, h[0]);
			if( !checkInput(h[0],id,h[2]) )
				this.client.sendActivityCommand("fr", id);
		}
	}

	private boolean checkInput(String inpfile, int inpid, String checksum) throws ExtensionException {
		MessageDigest complete;
		String check= null;
		try {
			complete = MessageDigest.getInstance("SHA1");
			
			try {
				// TODO: For some reason the Input-Directory is allready encoded in inpfile
				InputStream fis = new FileInputStream(inpfile);

				byte[] buffer = new byte[1024];
				int numRead;

				do {
					numRead = fis.read(buffer);
					if (numRead > 0) {
						complete.update(buffer, 0, numRead);
					}
				} while (numRead != -1);

				fis.close();
				byte[] b = complete.digest();

				check= ChecksumHelper.convToHex(b);
			} catch (FileNotFoundException e) {
				// This means we need to get the file!
				return false;
			} catch (IOException e) {
				// Also there is probably a mistake with the file -> get it
				return false;
			}
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
			throw new ExtensionException(e1);
		}
		
		if( check.equals(checksum) )
			return true;
		else
			return false;
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
