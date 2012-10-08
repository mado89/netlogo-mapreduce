package at.dobiasch.mapreduce.framework.controller;

import org.nlogo.api.LogoList;

import at.dobiasch.mapreduce.framework.RecordWriter;
import at.dobiasch.mapreduce.framework.TaskType;

/**
 * Helper class to store Data for a task
 * @author Martin Dobiasch
 *
 */
public class Data
{
	public long ID;
	public TaskType type;
	public String src;
	public String key;
	public long start;
	public long end;
	public RecordWriter dest;
	public LogoList values;
	
	/**
	 * 
	 * @param ID ID of the task
	 * @param type Task type
	 * @param src Name of the source file
	 * @param key Key (e.g starting byte)
	 * @param start Start Posisition
	 * @param end End Posisition
	 * @param dest File where the task writes its data
	 */
	public Data(long ID, TaskType type, String src, String key, long start, long end, RecordWriter dest)
	{
		this.ID = ID;
		this.type= type;
		this.src= src;
		this.key= key;
		this.start = start;
		this.end = end;
		this.dest= dest;
	}
	
	public Data(long ID, TaskType type, String src, String key, RecordWriter dest, long end)
	{
		this.ID = ID;
		this.type= type;
		this.src= src;
		this.key= key;
		this.start = 0;
		this.end = end;
		this.dest= dest;
	}
}
