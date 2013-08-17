package at.dobiasch.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.InputChecker;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.multi.MapRedHubNetManager;
import at.dobiasch.mapreduce.framework.multi.NodeManager;
import at.dobiasch.mapreduce.framework.partition.CheckPartData;
import at.dobiasch.mapreduce.framework.partition.Data;
import at.dobiasch.mapreduce.framework.task.TaskManager;

public class MultiNodeRun extends MapReduceRun
{
	private String world;
	private String modelpath;
	private Framework fw;
	private CheckPartData indata;
	private HostController controller;
	private HashMap<Data, Integer> partIDs;
	private String inpIDmsg;
	private NodeManager nodes;
	// private Map<String,List<String>> nodetasks;
	// private Map<String,List<String>> tasknodes;
	private long _id= 1;
	private TaskManager taskmanager;
	
	private long getID() {
		return _id++;
	}
	
	public MultiNodeRun(Framework fw, String world, String modelpath)
	{
		this.fw= fw;
		this.fw.setMaster(true);
		this.world= world;
		this.modelpath= modelpath;
		this.nodes= new NodeManager();
		// this.nodetasks= new HashMap<String,List<String>>();
		// this.tasknodes= new HashMap<String,List<String>>();
	}
	
	public void setup() throws ExtensionException
	{
		System.out.println("Setting up Multi Node Run");
		
		this.controller= new HostController( fw.getConfiguration().getMappers(),
				fw.getConfiguration().getReducers(),
				fw.getConfiguration().getMapper(), 
				fw.getConfiguration().getReducer(),
				fw.getSystemFileHandler(), 
				this.world, this.modelpath);
		
		this.fw.setHostController(this.controller);
		
		try
		{
			prepareInput();
			createInputIDs();
			prepareLocalMapper();
			// startHubNetManager();
		} catch (CompilerException e){
			throw new ExtensionException(e);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}

	/*
	private void startHubNetManager() throws CompilerException {
		fw.getHubNetManager().start();
	}*/

	@Override
	public double getMapProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getReduceProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void run() throws ExtensionException {
		System.out.println("MultiNodeRun::run");
		
		MapRedHubNetManager mgr= this.fw.getHubNetManager();
		mgr.runStarted();
		synchronized( mgr ) {
		while( mgr.getState() != MapRedHubNetManager.STATE_RUN )
		{
			try {
				mgr.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} }
		
		doMap();
			
			// doCollect();
			
			// doReduce();
			
			// doCollect();
	}
	
	private void doMap() throws ExtensionException{
		fw.getHubNetManager().sendConfigToClients(fw.getConfiguration());
		
		createInitialMapShedule();
	}

	private void createInitialMapShedule() throws ExtensionException {
        long partStart;
        long partEnd;
        int inpid;
        
        taskmanager= new TaskManager();
        
        taskmanager.setNodes(nodes);
        taskmanager.initMap(indata.getNumberOfPartitions(), controller);
        
        for(Data d : indata.values())
        {
        	System.out.println("Starting for " + d.key);
                try
                {
                        File file= new File(d.partitionfile);
                        BufferedReader in= new BufferedReader(new FileReader(file));
                        inpid= partIDs.get(d); 
                        
                        String line;
                        line= in.readLine(); //TODO: this assumes there is a line ... not good
                        partStart= Integer.parseInt(line);
                        while((line= in.readLine()) != null)
                        {
                                partEnd= Integer.parseInt(line);
                                // don't add an empty task
                                if( partStart < partEnd )
                                {
                                	// System.out.println(node);
                                    // System.out.println(d);
                                    
                                	// assignTask
                                	taskmanager.assignMapTask(getID(), partStart, partEnd, inpid, d.key);
                                    // tfn= assignMapTask(as, nn, tpn, getID(), node, partStart, partEnd, inpid, tfn);
                                }
                                
                                partStart= partEnd;
                        }
                        partEnd= d.lastpartitionend;
                        
                        // don't add an empty task
                        if( partStart < partEnd )
                        {
                        	taskmanager.assignMapTask(getID(), partStart, partEnd, inpid, d.key);
                        	// tfn= assignMapTask(as, nn, tpn, getID(), node, partStart, partEnd, inpid, tfn);
                        }
                } catch (FileNotFoundException e) {
                        e.printStackTrace();
                        throw new ExtensionException(e);
                } catch (IOException e) {
                        e.printStackTrace();
                        throw new ExtensionException(e);
                }
        }
        
        // Send out assignment
        System.out.println("Assignment created: " + taskmanager.getMapAssignmentString());
        try {
        	fw.getHubNetManager().broadcast(taskmanager.getMapAssignmentString());
		} catch (LogoException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
	
	/**
	 * Check the input and create Partitions, and input ids
	 * @throws Exception
	 */
	private void prepareInput() throws Exception
	{
		InputChecker c= new InputChecker(fw);
		c.check();
		this.indata= c.getData();
	}
	
	/**
     * Assign an ID to every input file and send it out to the clients
     * @throws LogoException (When broadcasting fails)
     */
	private void createInputIDs() throws LogoException
	{
		partIDs = new HashMap<Data, Integer>();
		int i = 0;
		for (at.dobiasch.mapreduce.framework.partition.Data d : indata.values())
		{
			partIDs.put(d, i++);
		}

		inpIDmsg = "INPIDS:[";
		for (Entry<Data, Integer> e : partIDs.entrySet())
		{
			inpIDmsg += e.getKey().key + ":" + e.getValue() + ",";
		}
		inpIDmsg = inpIDmsg.substring(0, inpIDmsg.length() - 1) + "]";
		
		fw.getHubNetManager().broadcast(inpIDmsg);
	}
	
	private void prepareLocalMapper() throws IOException, CompilerException
	{
		this.controller.prepareMappingStage();
	}

	public void newClient(String name) {
		nodes.add(name);
	}
	
	public void removeClient(String name) {
		// Tell TaskManger that node has failed
		// TODO: this means reschedule
		taskmanager.removeNode(name);
		
		nodes.removeClient(name);
	}
}
