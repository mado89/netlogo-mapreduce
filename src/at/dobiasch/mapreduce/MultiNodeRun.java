package at.dobiasch.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;

import at.dobiasch.mapreduce.framework.ChecksumHelper;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.InputChecker;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.multi.MapRedHubNetManager;
import at.dobiasch.mapreduce.framework.multi.NodeManager;
import at.dobiasch.mapreduce.framework.partition.CheckPartData;
import at.dobiasch.mapreduce.framework.partition.Data;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;
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
	private long _id= 1;
	private TaskManager taskmanager;
	Map<String,IntKeyVal> intdata= null;
	MessageDigest md= null;
	
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
		intdata= new HashMap<String,IntKeyVal>();
		taskmanager= new TaskManager();
		
		try {
			md = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
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

	@Override
	public double getMapProgress() {
		return this.taskmanager.getMapProgress(this.controller.getMapProgress());
	}

	@Override
	public double getReduceProgress() {
		return this.controller.getReduceProgress();
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
		
		collectMapResults();
		
		finishMapStage();
			
		doReduce();
			
		outputResult();
	}
	
	private void doMap() throws ExtensionException{
		fw.getHubNetManager().sendConfigToClients(fw.getConfiguration());
		
		createInitialMapShedule();
		requestResults();
	}

	private void requestResults() throws ExtensionException {
		try {
			fw.getHubNetManager().broadcast("ASSIGNED");
		} catch (LogoException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} 
	}
	
	private void finishMapStage() throws ExtensionException {
		boolean result= this.controller.waitForMappingStage();
		if( result == false)
			throw new ExtensionException("Mapping-Stage failed");
		
		this.controller.finishMappingStage();
		
		System.out.println("done mapping");
	}
	
	private void collectMapResults() {
		// no wait for hosts to send in finished tasks
		System.out.println("Master: sleeping");
		synchronized( this.taskmanager ) {
			while( !taskmanager.allMapsDone() ) {
				try {
					taskmanager.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("Master: finished");
	}
	
	private void doReduce() throws ExtensionException
	{
		Map<String,IntKeyVal> intd= this.controller.getIntermediateData(intdata);
		
		String[] kk= new String[intd.size()];
		
		intd.keySet().toArray(kk);
		
		System.out.println("Sorting");
		Arrays.sort(kk);
		
		try {
			System.out.println("Begin Reducing");
			this.controller.prepareReduceStage();
			
			for(int i= 0; i < kk.length; i++)
			{
				this.controller.addReduce(this.controller.getID(), kk[i], intd.get(kk[i]));
			}
			
			boolean result= this.controller.waitForReduceStage();
			if( result == false)
				throw new ExtensionException("Reduce-Stage failed");
			
			this.controller.finishReduceStage();
			System.out.println("Reducing ended");
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (CompilerException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}

	private void createInitialMapShedule() throws ExtensionException {
        long partStart;
        long partEnd;
        int inpid;
        
        try {
			fw.getHubNetManager().broadcast(inpIDmsg);
		} catch (LogoException e1) {
			e1.printStackTrace();
		}
        
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

	public void newMapResult(String source, String resultstring)
	{
		System.out.println("Resultstring: " + resultstring);
		String[] results= resultstring.split(">");
		String[] res;
		IntKeyVal h;
		String fn;
		String key;
		int i;
		
		for(i= 0; i < results.length; i++)
		{
			System.out.println("Results: " + results[i]);
			
			res= results[i].split(",");
			key= res[0].substring(1);
			
			h= intdata.get(key);
            
			try {
	            if( h == null )
	            {
	                md.update(key.getBytes());
	                fn= fw.getSystemFileHandler().addFile(ChecksumHelper.convToHex(md.digest()) + ".int");
	                h= new IntKeyVal(key,fn);
					
	                intdata.put(key, h);
	            }
	            
	            h.writeValue(res[1]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void nodeFinished(String node) {
		taskmanager.tasksDone(node);
	}
	
	private void outputResult() throws ExtensionException {
		String fn;
		System.out.println("Writing output");
		fn= this.fw.getConfiguration().getOutputDirectory();
		if( !fn.equals("") && !fn.endsWith("" + File.separatorChar)) //TODO: make it work for OS-Problems \\ on linux
			fn+= File.separatorChar;
		fn+= "output.txt";
		try {
			this.controller.mergeReduceOutput(fn);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
}
