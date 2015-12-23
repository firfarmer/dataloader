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
import com.salesforce.dataloader.model.Row;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/**
 * @author Kevin Ulrich
 */
public class BlobWriter implements DataWriter {

    //logger
    private static Logger logger = Logger.getLogger(BlobWriter.class);

    //Blob objects
    private CloudAppendBlob appendBlob;

    //Blob storage
    private CloudStorageAccount account;
    private CloudBlobClient client;
    private CloudBlobContainer container;
    private CloudBlobDirectory dir;
    private final String containerName = "testContainer";
    private final String blobName = "testFile";
    private ByteArrayOutputStream byteOut;
    private ObjectOutputStream objectOut;

    //URI
    private final String uri = ""; //Put your URI here // TODO: 12/22/2015 generate from makeURI

    private static StorageUri storageUri;

    //Class objects
    private final String fileName;
    private Config config;

    //Write vars
    private int offset;
    private int rowLength;

    //T/F connection is open
    private boolean open = false;

    //http://<storage-account-name>.blob.core.windows.net/<container-name>/<blob-name>

    public BlobWriter(String fileName, Config config){
        this.fileName = fileName;
        this.config = config;
    }

    private StorageUri makeURI(String uri) throws URISyntaxException{
        try {
            //this.uri = uri; @// TODO: 12/22/2015 implement
            URI javaUri = new URI(uri);
            return new StorageUri(javaUri);
        }catch(URISyntaxException e){
            String errMsg = Messages.getString("BlobWriter.URISyntax"); // TODO: 12/22/2015 implement  
            logger.error(errMsg, e);
            throw new URISyntaxException(e.getInput(), e.getReason());
        }
    }

    /**
     * Opens an Azure storage account and creates the appropriate container/directory/blob hierarchy
     *
     * @throws DataAccessObjectInitializationException
     */
    @Override
    public void open() throws DataAccessObjectInitializationException {
        try{
            storageUri = makeURI(uri);

            /**
             * We will want to replace this with:
             *
             * // Retrieve storage account from connection-string.
             * String storageConnectionString =
             * RoleEnvironment.getConfigurationSettings().get("StorageConnectionString");
             *
             * And just grab the connection string from the Config file
             */

            //Setup
            account = CloudStorageAccount.parse(uri);
            client = account.createCloudBlobClient();
            container = client.getContainerReference(containerName);
            container.createIfNotExists();
            byteOut = new ByteArrayOutputStream();
            objectOut = new ObjectOutputStream(byteOut);
            open = true;
        }catch (Exception e) {
            String errMsg = Messages.getString("BlobWriter.openWriting"); // TODO: 12/22/2015 implement 
            logger.error(errMsg, e);
        }
    }

    @Override
    public void close() {
        try {
            byteOut.close();
            objectOut.close();
            open = false;
        }catch(IOException e){
            String errMsg = Messages.getString("BlobWriter.closeWriting"); // TODO: 12/22/2015 implement 
            logger.error(errMsg, e);
        }
    }

    @Override
    public boolean writeRow(Row inputRow) throws DataAccessObjectException {
        try{
            appendBlob = container.getAppendBlobReference(blobName);
            byteOut.reset();
            objectOut.writeObject(inputRow);

            //If the Blob already exists append to it otherwise upload the buffer data
            if(BlobExistsOnCloud()) {
                appendBlob.appendFromByteArray(byteOut.toByteArray(), offset, rowLength);
            }else{
                appendBlob.uploadFromByteArray(byteOut.toByteArray(), offset, rowLength);
            }

            return true;
        }catch(Exception e) {
            String errMsg = Messages.getString("BlobWriter.errorWriting"); // TODO: 12/22/2015 implement
            logger.error(errMsg, e);
            return false;
        }
    }

    private boolean BlobExistsOnCloud() throws StorageException{
        try {
            return client.getContainerReference(containerName).getBlockBlobReference(blobName).exists();
        }catch(Exception e){
            logger.error(e.getMessage());
            throw new StorageException("Storage Exception", e.getMessage(),
                    -1, new StorageExtendedErrorInformation(), e); //Not sure how to simplify this, is generic right now
        }
    }

    @Override
    public void setColumnNames(List<String> columnNames) throws DataAccessObjectInitializationException {

    }

    @Override
    public boolean writeRowList(List<Row> inputRowList) throws DataAccessObjectException {
        return false;
    }

    @Override
    public void checkConnection() throws DataAccessObjectInitializationException {
        open();
        close();
    }

    @Override
    public List<String> getColumnNames() {
        return null;
    }

    @Override
    public int getCurrentRowNumber() {
        return 0;
    }

    public boolean isOpen(){ return open; }
}
