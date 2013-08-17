package at.dobiasch.mapreduce.framework.multi;

import java.util.ArrayList;

import org.nlogo.agent.Observer;
import org.nlogo.api.CompilerException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.api.SimpleJobOwner;
import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Procedure;

import scala.collection.Iterable;

public class MapRedHubNetManager {
	private HeadlessWorkspace ws;
	private SimpleJobOwner owner;
	private Procedure mgr;
	private int state= STATE_UNINIT;
	
	public final static int STATE_UNINIT= 0;
	public final static int STATE_INIT= 1;
	public final static int STATE_RUNSTARTED= 2;
	public final static int STATE_RUN= 3;
	public final static int STATE_RUNFINISHED= 4;
	
	private class HelperT extends Thread {
		public HelperT() {
			super();
		}
		
		public void run() {
			System.out.println("Start MapRedHubNetManager");
			state= STATE_INIT;
			ws.runCompiledCommands(owner, mgr);
		}
	}
	
	public int getState() {
		return this.state;
	}
	
	public MapRedHubNetManager(String model) throws CompilerException, LogoException {
		init(model);
	}
	
	private void init(String model) throws CompilerException, LogoException {
		ws= HeadlessWorkspace.newInstance();
		
		ws.open(model);
		
		owner= new SimpleJobOwner("MapReduce", ws.world.mainRNG,Observer.class);
		
		// StartUp Hubnet
		ws.getHubNetManager().reset();
		ArrayList<Object> list= new ArrayList<Object>();
		ws.getHubNetManager().setClientInterface("MAPREDUCE", 
				scala.collection.JavaConversions.collectionAsScalaIterable(list));
		
		mgr= ws.compileCommands("mapreduce:__mrhubnetmgr");
		System.out.println("HubNet Manager initialized");
	}
	
	public void broadcast(String message) throws LogoException {
		ws.hubnetManager().broadcast(message);
	}
	
	@Deprecated
	public HubNetInterface getInterface() {
		return ws.hubnetManager();
	}
	
	public Iterable<String> getNodes()
	{
		return this.ws.hubnetManager().clients();
	}
	
	public void start() {
		new HelperT().start();
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
}
