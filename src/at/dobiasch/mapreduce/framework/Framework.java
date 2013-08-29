package at.dobiasch.mapreduce.framework;

import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.inputparser.IInputParser;
import at.dobiasch.mapreduce.framework.multi.MapRedHubNetManager;
import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition;
import at.dobiasch.mapreduce.framework.partition.ParallelPartitioner;


public class Framework
{
	private Configuration  config;
	private boolean        masterp;
	private HostController controller;
	// private String         sysdir;
	private SysFileHandler sysfileh;
	private IInputParser   inp;
	private MapReduceRun run;
	private boolean        multinode;
	private MapRedHubNetManager hubnetmgr;
	private int job;
	
	public Framework() throws ExtensionException
	{
		this.config= new Configuration();
		this.masterp= false;
		sysfileh= new SysFileHandler("./tmpdir" + ((int)(Math.random() * 100))  + "/");
		
		this.run= null;
		this.multinode= false;
		
		this.job= -1; //-1 because then we can see if a job was started. First Job has number 0
		
		// Don't initialize Inputparser here. Config will change reference won't be updated
		// this.inp= new TextInputFormat();
		// this.inp.init(this.config.getValueSeperator());
	}
	
	public Configuration getConfiguration()
	{
		return this.config;
	}
	
	/**
	 * Returns wheter or not this node is the master
	 * @return 
	 */
	public boolean isMaster()
	{
		return this.masterp;
	}

	/**
	 * Set wheter this node it the master (default = false)
	 * @param flag
	 */
	public void setMaster(boolean flag)
	{
		this.masterp= flag;
	}
	
	/**
	 * Get the Number of connected clients
	 * @param hubnet Reference to HubNetInterface
	 * @return Number of connected clients
	 */
	public int getNNodes(HubNetInterface hubnet)
	{
		return hubnet.clients().size();
	}
	
	public HostController getTaskController()
	{
		return controller;
	}

	public SysFileHandler getSystemFileHandler()
	{
		return sysfileh;
	}

	public void setHostController(HostController controller)
	{
		this.controller= controller;
	}

	public ICheckAndPartition getNewPartitioner() throws Exception
	{
		ICheckAndPartition part= new ParallelPartitioner();
		part.init(this.getSystemFileHandler(), 1); //TODO: hadoop seems to add a task for every line
		part.setCheck(false);
		return part;
	}

	public IInputParser newInputParser()
	{
		this.inp= this.config.getParser().newInstance();
		
		return this.inp; //.createParser(data);
	}

	public void setRun(MapReduceRun run)
	{
		this.run= run;
	}
	
	public boolean isRunning()
	{
		if( this.isMaster() )
		{
			if(this.run != null)
				return this.run.isRunning();
			else
				return false;
		}
		else // TODO: implement me!
			return false;
	}

	public double getMapProgress()
	{
		if( this.run != null )
			return this.run.getMapProgress();
		else
			return 0;
	}
	
	public double getReduceProgress()
	{
		if( this.run != null )
			return this.run.getReduceProgress();
		else
			return 0;
	}
	
	public boolean isMultiNode() {
		return multinode;
	}

	public void setMultiNode(boolean b) {
		this.multinode= b;
	}

	public MapReduceRun getRun() {
		return this.run;
	}

	public void setHubNetManager(MapRedHubNetManager manager) {
		this.hubnetmgr= manager;	
	}

	public MapRedHubNetManager getHubNetManager() {
		return this.hubnetmgr;
	}

	public int getJobNr() {
		return this.job;
	}

	public void newJob() {
		this.job++;
	}
	
	/*public String getSystemDir()
	{
		return sysdir; // TODO: not hardcoded is maybe better ...
	}*/
}
