package at.dobiasch.mapreduce.framework.controller;

import at.dobiasch.mapreduce.framework.RecordWriter;
import at.dobiasch.mapreduce.framework.TaskType;

public class Data
{
	public long ID;
	public TaskType type;
	public String src;
	public String key;
	public long start;
	public long end;
	public RecordWriter dest;
	
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
