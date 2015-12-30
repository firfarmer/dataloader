/*
 * Copyright (c) 2015, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.dataloader.dao.blob;

import com.salesforce.dataloader.config.Config;
import com.salesforce.dataloader.config.Messages;
import com.salesforce.dataloader.dao.DataWriter;
import com.salesforce.dataloader.exception.DataAccessObjectException;
import com.salesforce.dataloader.exception.DataAccessObjectInitializationException;
import com.salesforce.dataloader.exception.ParameterLoadException;
import com.salesforce.dataloader.model.Row;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/**
 * @author Kevin Ulrich
 */
public class BlobWriter implements DataWriter {

    //logger
    private static Logger logger = Logger.getLogger(BlobWriter.class);

    private List<String> columnNames = new ArrayList<>();

    //Blob objects
    private CloudAppendBlob appendBlob;

    //Blob storage
    private CloudStorageAccount account;
    private CloudBlobClient client;
    private CloudBlobContainer container;

    //URI
    private String uri;

    //Class objects
    private final String fileName;
    private String containerName;
    private Config config;
    private int uploadLines;
    private int linesToBeUploaded;
    private final boolean capitalizedHeadings;
    private int currentRowNumber = 0;
    private boolean hasWritten = false;
    private String uploadString;
    private int records;

    //T/F connection is open
    private boolean open = false;


    public BlobWriter(String fileName, Config config) {
        this.fileName = fileName;
        this.config = config;
        this.capitalizedHeadings = true;
    }

    /**
     * Opens an Azure storage account and creates the appropriate container/directory/blob hierarchy
     *
     * @throws DataAccessObjectInitializationException
     */
    @Override
    public void open() throws DataAccessObjectInitializationException {
        try {
            //Setup
            uri = getURI();
            account = CloudStorageAccount.parse(uri);
            client = account.createCloudBlobClient();
            containerName = getContainerName();
            container = client.getContainerReference(containerName);
            container.createIfNotExists();
            uploadLines = getUploadLines();
            linesToBeUploaded=0;
            uploadString="";
            records=0;

            open = true;
        } catch (Exception e) {
            String errMsg = Messages.getString("BlobWriter.openWriting"); // TODO: 12/22/2015 implement 
            logger.error(errMsg, e);
        }
    }

    @Override
    public void close() {
        try {
            linesToBeUploaded=0;
            uploadString="";
            open = false;
        } catch (Exception e) {
            String errMsg = Messages.getString("BlobWriter.closeWriting"); // TODO: 12/22/2015 implement 
            logger.error(errMsg, e);
        }
    }

    private void writeHeaderRow() throws DataAccessObjectException {
        try {

            uploadHeaderColumns(this.columnNames);

        } catch (Exception e) {
            String errMsg = Messages.getString("BlobWriter.errorWriting"); // TODO: 12/23/2015 implement 
            logger.error(errMsg, e);
            throw new DataAccessObjectInitializationException(errMsg, e);
        }

    }

    @Override
    public boolean writeRow(Row inputRow) throws DataAccessObjectException {
        try {
            uploadColumns(columnNames, inputRow);

            currentRowNumber++; //What is this for?
            return true;
        } catch (Exception e) {
            String errMsg = Messages.getString("BlobWriter.errorWriting"); // TODO: 12/22/2015 implement
            logger.error(errMsg, e);
            return false;
        }
    }

    private void uploadHeaderColumns(List<String> columnNames) throws IOException {
        BlobColumnVisitor visitor = new BlobColumnVisitor();

        uploadToAzure(visitor.visitHeader(columnNames, capitalizedHeadings));
    }

    private void uploadColumns(List<String> columnNames, Row inputRow) throws IOException {
        BlobColumnVisitor visitor = new BlobColumnVisitor();

        uploadString += visitor.visit(columnNames, inputRow);
        linesToBeUploaded++;

        //We check if we have accumulated enough lines, if so then we upload to Azure, with an alternative limit of
        //the number of records we have, in case we can't fulfill uploadLines
        if(linesToBeUploaded==uploadLines || linesToBeUploaded==records) {
            uploadToAzure(uploadString);
            linesToBeUploaded=0;
        }
    }

    private boolean uploadToAzure(String s) {
        try {
            //Open connection to Azure server
            appendBlob = container.getAppendBlobReference(fileName);

            if (!BlobExistsOnCloud() || BlobExistsOnCloud() && !hasWritten)
                appendBlob.createOrReplace();
                hasWritten = true;

            appendBlob.appendText(s);

            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false; //An error occured and we did not upload to the server
        }
    }

    private boolean BlobExistsOnCloud() throws StorageException {
        try {
            return client.getContainerReference(containerName).getBlockBlobReference(fileName).exists();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new StorageException("Storage Exception", e.getMessage(),
                    -1, new StorageExtendedErrorInformation(), e); //Not sure how to simplify this, is generic right now
        }
    }

    /*
    config getters
     */
    private String getURI() {
        return config.getURI();
    }

    private String getContainerName() {
        return config.getContainerName();
    }

    private int getUploadLines() throws ParameterLoadException { return config.getUploadLines(); }

    @Override
    public void setColumnNames(List<String> columnNames) throws DataAccessObjectInitializationException {
        try {
            if (columnNames == null || columnNames.isEmpty()) {
                String errMsg = Messages.getString("BlobFileDAO.errorOpenNoHeaderRow");
                logger.error(errMsg);
                throw new DataAccessObjectInitializationException(errMsg);
            }
            // save column names
            this.columnNames = columnNames;

            writeHeaderRow();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public boolean writeRowList(List<Row> inputRowList) throws DataAccessObjectException {
        boolean success = true; //priming

        records = inputRowList.size();

        for (Row row : inputRowList)
            success = writeRow(row);

        return success;
    }

    @Override
    public void checkConnection() throws DataAccessObjectInitializationException {
        open();
        close();
    }

    @Override
    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    public boolean isOpen() {
        return open;
    }

    //We don't use this cause it's static and therefore stupid
    static private void visitColumns(List<String> columnNames, Row inputRow, BlobColumnVisitor visitor) throws IOException {

    }
}
