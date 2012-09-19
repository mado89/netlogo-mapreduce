package at.dobiasch.mapreduce.framework.controller;

import java.io.FileWriter;

import at.dobiasch.mapreduce.framework.TaskType;

public class Data
{
	public long ID;
	public TaskType type;
	public String src;
	public String key;
	public long start;
	public long end;
	public FileWriter dest;
	
	public Data(long ID, TaskType type, String src, String key, long start, long end)
	{
		this.ID = ID;
		this.type= type;
		this.src= src;
		this.key= key;
		this.start = start;
		this.end = end;
		this.dest= null;
	}
	
	public Data(long ID, TaskType type, String src, String key, FileWriter dest, long end)
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
