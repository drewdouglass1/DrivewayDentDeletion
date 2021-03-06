package com.ibm.demo;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;

public class PostQuote_UPDATE extends MbJavaComputeNode {
	static private Integer toInteger(Object value) {
		if(value == null) return null;
    	try {
    		return Integer.valueOf(value.toString());
    	} catch (NumberFormatException e) {
    		return null;
    	}
	}
	
	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		MbOutputTerminal alt = getOutputTerminal("alternate");

		MbMessage inMessage = inAssembly.getMessage();

		// create new empty message
		MbMessage outMessage = new MbMessage();
		MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly,
				outMessage);

		try {
			// optionally copy message headers
			copyMessageHeaders(inMessage, outMessage);
			// ----------------------------------------------------------
			// Add user code below
			MbElement outRoot = outMessage.getRootElement();
			MbElement outJsonRoot = outRoot.createElementAsLastChild(MbJSON.PARSER_NAME);
			MbElement outJsonData = outJsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "QuoteID", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/QuoteID").getValue());
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Name",
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/Name").getValue());
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "EMail", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/EMail").getValue());
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Address", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/Address").getValue());
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "USState", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/USState").getValue());
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "LicensePlate", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/LicensePlate").getValue());						
			String backEndVersions = 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Accident/Reply/Root/JSON/Data/Version").getValue() + "," +
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Bumper/Reply/Root/JSON/Data/Version").getValue() + "," +
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Crumpled/Reply/Root/JSON/Data/Version").getValue();					
			outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Versions", backEndVersions);					
			MbElement RepairQuotes = outJsonData.createElementAsLastChild(MbJSON.ARRAY, "RepairQuotes", null);
			MbElement FirstArrayItem = RepairQuotes.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.ARRAY_ITEM_NAME, null);
			MbElement SecondArrayItem = RepairQuotes.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.ARRAY_ITEM_NAME, null);
			MbElement ThirdArrayItem = RepairQuotes.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.ARRAY_ITEM_NAME, null);			
			FirstArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "VendorName", "AcmeAutoAccidents");
			FirstArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "CostEstimate",
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Accident/Reply/Root/JSON/Data/EstimatedCost").getValue());
			FirstArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "EarliestAppointmentDate",			
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Accident/Reply/Root/JSON/Data/EarliestStartDate").getValue());
			SecondArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "VendorName", "BernieBashedBumpers");
			SecondArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "CostEstimate", 
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Bumper/Reply/Root/JSON/Data/EstimatedCost").getValue());										
			SecondArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "EarliestAppointmentDate",
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Bumper/Reply/Root/JSON/Data/EarliestStartDate").getValue());					
			ThirdArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "VendorName", "ChrisCrumpledCars");
			ThirdArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "CostEstimate",
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Crumpled/Reply/Root/JSON/Data/EstimatedCost").getValue());
			ThirdArrayItem.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "EarliestAppointmentDate",
				inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Crumpled/Reply/Root/JSON/Data/EarliestStartDate").getValue());			
			// Set HTTP info
			MbElement LocalEnv = outAssembly.getLocalEnvironment().getRootElement();
			MbElement Destination = LocalEnv.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Destination", null);
			MbElement HTTP = Destination.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "HTTP", null);
			HTTP.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "RequestIdentifier", inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/HTTP/RequestIdentifier").getValue());

			// To test without access to postgresql, you can comment out the user code below here ...
			
			// Update quote information in the Postgresql database using the reply data ...			
	        Connection conn = getJDBCType4Connection("{DefaultPolicies}:PostgresqlPolicy", JDBC_TransactionType.MB_TRANSACTION_AUTO);
	        
	        Object acmecost = toInteger(inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Accident/Reply/Root/JSON/Data/EstimatedCost").getValue());
	        Object acmedate = inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Accident/Reply/Root/JSON/Data/EarliestStartDate").getValue();
	        Object berniecost = toInteger(inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Bumper/Reply/Root/JSON/Data/EstimatedCost").getValue()); 
    		Object berniedate = inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Bumper/Reply/Root/JSON/Data/EarliestStartDate").getValue();
    		Object chriscost = toInteger(inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Crumpled/Reply/Root/JSON/Data/EstimatedCost").getValue());
			Object chrisdate = inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Replies/Crumpled/Reply/Root/JSON/Data/EarliestStartDate").getValue();
			Object quoteid = inAssembly.getMessage().getRootElement().getFirstElementByPath("ComIbmGroupCompleteNode/Group/Context/QuoteID").getValue();

			String sql = "UPDATE quotes SET acmecost=?, acmedate=?::date, berniecost=?, berniedate=?::date, chriscost=?, chrisdate=?::date WHERE quoteid=?";

	        PreparedStatement pstmt = conn.prepareStatement(sql);
	        pstmt.setObject(1, acmecost);
	        pstmt.setObject(2, acmedate);
	        pstmt.setObject(3, berniecost);
	        pstmt.setObject(4, berniedate);
	        pstmt.setObject(5, chriscost);
	        pstmt.setObject(6, chrisdate);
	        pstmt.setObject(7, quoteid);
	        	        
	        pstmt.executeUpdate();

	        // End of user code
			// ----------------------------------------------------------
		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);
	}

	public void copyMessageHeaders(MbMessage inMessage, MbMessage outMessage)
			throws MbException {
		MbElement outRoot = outMessage.getRootElement();

		// iterate though the headers starting with the first child of the root
		// element, stopping before the last child (body)
		MbElement header = inMessage.getRootElement().getFirstChild();
		while (header != null && header.getNextSibling() != null) {
			// copy the header and add it to the out message
			MbElement newHeader = outRoot.createElementAsLastChild(header
					.getParserClassName());
			newHeader.setName(header.getName());
			newHeader.copyElementTree(header);
			// move along to next header
			header = header.getNextSibling();
		}
	}

	/**
	 * onPreSetupValidation() is called during the construction of the node
	 * to allow the node configuration to be validated.  Updating the node
	 * configuration or connecting to external resources should be avoided.
	 *
	 * @throws MbException
	 */
	@Override
	public void onPreSetupValidation() throws MbException {
	}

	/**
	 * onSetup() is called during the start of the message flow allowing
	 * configuration to be read/cached, and endpoints to be registered.
	 *
	 * Calling getPolicy() within this method to retrieve a policy links this
	 * node to the policy. If the policy is subsequently redeployed the message
	 * flow will be torn down and reinitialized to it's state prior to the policy
	 * redeploy.
	 *
	 * @throws MbException
	 */
	@Override
	public void onSetup() throws MbException {
	}

	/**
	 * onStart() is called as the message flow is started. The thread pool for
	 * the message flow is running when this method is invoked.
	 *
	 * @throws MbException
	 */
	@Override
	public void onStart() throws MbException {
	}

	/**
	 * onStop() is called as the message flow is stopped. 
	 *
	 * The onStop method is called twice as a message flow is stopped. Initially
	 * with a 'wait' value of false and subsequently with a 'wait' value of true.
	 * Blocking operations should be avoided during the initial call. All thread
	 * pools and external connections should be stopped by the completion of the
	 * second call.
	 *
	 * @throws MbException
	 */
	@Override
	public void onStop(boolean wait) throws MbException {
	}

	/**
	 * onTearDown() is called to allow any cached data to be released and any
	 * endpoints to be deregistered.
	 *
	 * @throws MbException
	 */
	@Override
	public void onTearDown() throws MbException {
	}

}
