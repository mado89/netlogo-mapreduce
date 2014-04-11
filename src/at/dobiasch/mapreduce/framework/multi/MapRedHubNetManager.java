package at.dobiasch.mapreduce.framework.multi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.workspace.AbstractWorkspace;

import ch.randelshofer.quaqua.ext.base64.Base64;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.MultiNodeRun;
import at.dobiasch.mapreduce.framework.ChecksumHelper;
import at.dobiasch.mapreduce.framework.Configuration;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

import scala.collection.Iterable;

public class MapRedHubNetManager {
	private AbstractWorkspace ws;
	private int state= STATE_UNINIT;
	private HelperT handler;
	
	public final static int STATE_UNINIT= 0;
	public final static int STATE_INIT= 1;
	public final static int STATE_RUNSTARTED= 2;
	public final static int STATE_RUN= 3;
	public final static int STATE_RUNFINISHED= 4;
	
	private class HelperT extends Thread {
		private HubNetInterface hubnet;
		private MultiNodeRun run;
		private Framework fw;
		private BlockingQueue<String> msgtosend;
		
		public HelperT(HubNetInterface hubnet, Framework fw) {
			super();
			this.hubnet= hubnet;
			this.fw= fw;
			this.msgtosend= new LinkedBlockingQueue<String>();
		}
		
		public void run() {
			System.out.println("Start MapRedHubNetManager");
			state= STATE_INIT;
			// ws.runCompiledCommands(owner, mgr);
			
			boolean runIt = true;

			while (runIt) {
				state= fw.getHubNetManager().getState();
				// System.out.println("Message loop " + state);
				
				try {
				switch( state )
				{
					case MapRedHubNetManager.STATE_INIT: // do nothing
						idleFetch();
						break;
					case MapRedHubNetManager.STATE_RUNSTARTED:
						prepareRun();
						break;
					case MapRedHubNetManager.STATE_RUN:
						feedNodes();
						break;
					case MapRedHubNetManager.STATE_RUNFINISHED:
						// TODO: what to do here?
						break;
				}
				
				Thread.sleep(500);
				} catch (InterruptedException e) {
	                e.printStackTrace();
				} catch (ExtensionException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void idleFetch() throws ExtensionException
		{
			try {
				if (hubnet.messageWaiting())
				{
					hubnet.fetchMessage();
					String name = hubnet.getMessageSource();

					if (hubnet.enterMessage()) {
						System.out.println("New Client " + name);
					} else if (hubnet.exitMessage()) {
						System.out.println("Client left " + name);
					} else {
						System.out.println("other message");
					}
				} else
					System.out.println("No msg");
			} catch (LogoException e) {
				e.printStackTrace();
				throw new ExtensionException(e);
			}
		}

		private void prepareRun() throws ExtensionException
		{
			MapReduceRun mrrun= fw.getRun();
			if( mrrun.getClass() != MultiNodeRun.class)
				throw new ExtensionException("Needed a MultiNodeRun but got a " + mrrun.getClass());
			run= (MultiNodeRun) mrrun;
			
			scala.collection.Iterator<String> it= hubnet.clients().iterator();
			String node;
			
			System.out.println("Feeding nodes");
			
			while(it.hasNext())
			{
				node= it.next();
				run.newClient(node);
			}
			
			fw.getHubNetManager().doneFeeding();
		}

		private void feedNodes() throws ExtensionException
		{
	        try {
	                if( hubnet.messageWaiting() )
	                {
	                        hubnet.fetchMessage();
	                        if( hubnet.enterMessage() )
	                        {
	                        	System.out.println("New Client");
	                            String name= hubnet.getMessageSource();
	                            run.newClient(name);
	                        }
	                        else if( hubnet.exitMessage() )
	                        {
	                        	String name = hubnet.getMessageSource();
	                        	System.out.println("Client left '" + name + "'");
								run.removeClient(name);
	                        }
	                        else if( hubnet.getMessageTag().equals("results") )
	                        {
	                        	System.out.println("Tag: " + hubnet.getMessageTag());
	                        	System.out.println("Source: " + hubnet.getMessageSource());
	                        	System.out.println("Message: " + hubnet.getMessage());
	                        	byte[] decoded = Base64.decode((String) hubnet.getMessage());
	                        	String res= null;
								try {
									res = new String(decoded, "UTF-8");
								} catch (UnsupportedEncodingException e) {
									e.printStackTrace();
								}
	                        	
								if( run.newMapResult(hubnet.getMessageSource(), res) )
									run.nodeFinished(hubnet.getMessageSource());
								else
									run.removeClient(hubnet.getMessageSource());
	                        }
	                        else if( hubnet.getMessageTag().equals("fr") )
	                        {
	                        	System.out.println("Tag: " + hubnet.getMessageTag());
	                        	System.out.println("Source: " + hubnet.getMessageSource());
	                        	System.out.println("Message: " + hubnet.getMessage());
	                        	// if( hubnet.getMessage().getClass() == Integer.class) {
	                        	run.fileRequest(hubnet.getMessageSource(), 
	                        			(Integer) hubnet.getMessage());
	                        }
	                        else
	                        {
	                        	System.out.println("Tag: " + hubnet.getMessageTag());
	                        	System.out.println("Source: " + hubnet.getMessageSource());
	                        	System.out.println("Message: " + hubnet.getMessage());
	                        }
	                }
	                
	                String msg= msgtosend.poll();
	                if( msg != null )
	                	broadcast(msg);
	        } catch (LogoException e) {
	                e.printStackTrace();
	                throw new ExtensionException(e);
	        }
		}

		public synchronized void queueMsg(String msg) {
			try {
				msgtosend.put(msg);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public int getState() {
		return this.state;
	}
	
	public MapRedHubNetManager(String model, AbstractWorkspace workspace) throws CompilerException, LogoException {
		ws= workspace;
		init(model);
	}
	
	private void init(String model) throws CompilerException, LogoException {
		// ws= HeadlessWorkspace.newInstance();
		
		// ws.open(model);
		
		// StartUp Hubnet
		ws.getHubNetManager().reset();
		ArrayList<Object> list= new ArrayList<Object>();
		ws.getHubNetManager().setClientInterface("MAPREDUCE", 
				scala.collection.JavaConversions.collectionAsScalaIterable(list));
		System.out.println(ws.getHubNetManager().toString());
	}
	
	public synchronized void broadcast(String message) throws LogoException {
		ws.hubnetManager().broadcast(message);
	}
	
	public synchronized void sayGoodBy(String to) throws LogoException {
		ws.hubnetManager().send(to, "ExitMessage", "Shutting Down.");
	}
	
	@Deprecated
	public HubNetInterface getInterface() {
		return ws.hubnetManager();
	}
	
	/**
	 * Send the configuration to all clients
	 * @param configuration 
	 * @throws FrameworkException 
	 */
	public void sendConfigToClients(Configuration configuration)
	{
		HubNetInterface hubnet= ws.hubnetManager();
		
		// Create client set
		scala.collection.Seq<String> clients= hubnet.clients().toSeq();
		
		Map<String,String> fields= configuration.getChangedFields();
		if( fields.keySet().size() > 0 )
		{		
			System.out.println(clients);
			System.out.println(fields);
			
			String config= "CONFIG: [";
			Collection<String> vals= fields.values();
			Iterator<String> it= vals.iterator();
			for(String key : fields.keySet())
			{
				config+= key + "=" + it.next() + ",";
			}
			config+= "]";
			
			try
			{
				hubnet.broadcast(config);
			} catch (LogoException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Iterable<String> getNodes()
	{
		return this.ws.hubnetManager().clients();
	}
	
	public void start() throws ExtensionException {
		handler= new HelperT(ws.getHubNetManager(), FrameworkFactory.getInstance());
		handler.start();
		System.out.println("HubNetManager started");
	}

	public synchronized void doneFeeding() {
		this.state= STATE_RUN;
		notifyAll();
	}

	public synchronized void runStarted() {
		this.state= STATE_RUNSTARTED;
		notifyAll();
	}

	public void sendFile(String filename, int inpid, String node) throws ExtensionException {
		System.out.println("Send file " + filename);
		
		try {
			InputStream fis= null;
			fis = new FileInputStream( filename);
			
			byte[] buffer = new byte[1024];
			int numRead;
			int i= 0;

			do {
				numRead = fis.read(buffer);
				if (numRead > 0) {
					// System.out.println(numRead);
					this.handler.queueMsg("f-" + inpid + "-" + i + "-" + Base64.encodeBytes(buffer, 0, numRead));
					i++;
				}
			} while (numRead != -1);

			fis.close();
			//end of file
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
}
