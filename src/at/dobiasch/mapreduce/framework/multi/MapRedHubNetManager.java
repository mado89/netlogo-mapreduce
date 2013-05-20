package at.dobiasch.mapreduce.framework.multi;

import java.util.ArrayList;

import org.nlogo.agent.Observer;
import org.nlogo.api.CompilerException;
import org.nlogo.api.LogoException;
import org.nlogo.api.SimpleJobOwner;
import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Procedure;

public class MapRedHubNetManager {
	private HeadlessWorkspace ws;
	private SimpleJobOwner owner;
	private Procedure mgr;
	
	private class HelperT extends Thread {
		public HelperT() {
			super();
		}
		
		public void run() {
			System.out.println("Start MapRedHubNetManager");
			ws.runCompiledCommands(owner, mgr);
		}
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
	
	public void start() {
		new HelperT().start();
		System.out.println("HubNetManager started");
	}
}
