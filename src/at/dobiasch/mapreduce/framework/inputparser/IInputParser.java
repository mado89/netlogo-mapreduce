package at.dobiasch.mapreduce.framework.inputparser;

import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.LogoObject;
import at.dobiasch.mapreduce.framework.controller.Data;

public interface IInputParser
{
	public void init(String valueseperator);
	
	// public IInputParser createParser(Data data);
	
	public void parseInput(Data data) throws ExtensionException;
	
	public LogoObject getValues();
	public String   getKey();

	public IInputParser newInstance();
}
