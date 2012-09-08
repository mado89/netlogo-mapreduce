package at.dobiasch.mapreduce.framework.partition;

import java.util.Map;

import at.dobiasch.mapreduce.framework.FrameworkException;

public interface ICheckAndPartition
{
	/**
	 * Init the Checker and Partitioner
	 * @param sysdir Path to the dir where the framework can store the system files
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 */
	public void init(String sysdir, int blocksize) throws Exception;
	
	/**
	 * Add a File
	 * @param path
	 */
	public void addFile(String path);
	
	/**
	 * 
	 * @return map containing all checksums key:path to the file
	 * @throws FrameworkException
	 */
	public Map<String,String> getChecksums() throws FrameworkException;
}
