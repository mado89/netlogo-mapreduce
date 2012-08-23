package at.dobiasch.mapreduce.framework;


public class Framework
{
	private Configuration config;
	private boolean       masterp;
	
	public Framework()
	{
		this.config= new Configuration();
		this.masterp= false;
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
	public void sendConfigToClients() throws FrameworkException
	{
		if( this.masterp == false) throw new FrameworkException("Must be master to send config");
		
		
	}
}
