package at.dobiasch.mapreduce.framework.partition;

import java.util.Map;

import at.dobiasch.mapreduce.framework.FrameworkException;

public interface ICheckAndPartition
{
	public class CheckPartData
	{
		public String key;
		public String checksum;
		public int    numpartitions;
		public String partitionfile;
		
		public String toString()
		{
			return "[" + key + "," + checksum + "," + numpartitions + "," + partitionfile + "]";
		}
	}
	
	/**
	 * Init the Checker and Partitioner
	 * @param sysdir Path to the dir where the framework can store the system files
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 */
	public void init(String sysdir, int blocksize) throws Exception;
	
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
	public Map<String,String> getChecksums() throws FrameworkException;
	
	/**
	 * Get all Files and their data
	 * @return map containing all checksums key:path to the file
	 * @throws FrameworkException
	 */
	public Map<String,CheckPartData> getData() throws FrameworkException;
}
