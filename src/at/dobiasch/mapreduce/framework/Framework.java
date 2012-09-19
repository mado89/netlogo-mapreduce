package at.dobiasch.mapreduce.framework;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;

import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.controller.HostTaskController;


public class Framework
{
	private Configuration  config;
	private boolean        masterp;
	private HostController controller;
	// private String         sysdir;
	private SysFileHandler sysfileh;
	
	public Framework()
	{
		this.config= new Configuration();
		this.masterp= false;
		sysfileh= new SysFileHandler("/home/martin/DA/tmpdir");
		// this.controller= new HostTaskController(sysfileh);
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
	 * Send the configuration to all clients
	 * @throws FrameworkException 
	 */
	public void sendConfigToClients(HubNetInterface hubnet) throws FrameworkException
	{
		if( this.masterp == false) throw new FrameworkException("Must be master to send config");
		
		// Create client set
		scala.collection.Seq<String> clients= hubnet.clients().toSeq();
		
		this.getConfiguration().setInputDirectory("asdf");
		Map<String,String> fields= this.getConfiguration().getChangedFields();
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
	
	/*public String getSystemDir()
	{
		return sysdir; // TODO: not hardcoded is maybe better ...
	}*/
}
