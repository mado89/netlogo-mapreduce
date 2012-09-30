package at.dobiasch.mapreduce.framework.inputparser;

import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;

import at.dobiasch.mapreduce.framework.controller.Data;

public interface IInputParser
{
	public void init(String valueseperator);
	
	// public IInputParser createParser(Data data);
	
	public LogoList parseInput(Data data) throws ExtensionException;
}
