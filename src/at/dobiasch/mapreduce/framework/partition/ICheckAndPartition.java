package at.dobiasch.mapreduce.framework.partition;

import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.SysFileHandler;

public interface ICheckAndPartition
{
	/**
	 * Init the Checker and Partitioner
	 * @param sysfileh
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 */
	public void init(SysFileHandler sysfileh, int blocksize) throws Exception;
	
	/**
	 * Set wheter checksums of the files should be built or not
	 * Usefull for singenode (Time saving)
	 * @param check
	 */
	public void setCheck(boolean check);
	
	/**
	 * Add a File
	 * @param path
	 */
	public void addFile(String path);
	
	/**
	 * Get all Files and their checksums
	 * @return map containing all checksums key:path to the file
	 * @throws FrameworkException
	 */
	// public Map<String,String> getChecksums() throws FrameworkException;
	
	/**
	 * Get all Files and their data
	 * @return map containing all checksums key:path to the file
	 * @throws FrameworkException
	 */
	public CheckPartData getData() throws FrameworkException;
}
