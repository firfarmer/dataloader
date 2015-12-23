package com.salesforce.dataloader.dao.blob;

import com.salesforce.dataloader.config.Messages;
import com.salesforce.dataloader.dao.DataAccessObject;
import com.salesforce.dataloader.dao.DataWriter;
import com.salesforce.dataloader.exception.DataAccessObjectException;
import com.salesforce.dataloader.exception.DataAccessObjectInitializationException;
import com.salesforce.dataloader.model.Row;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/**
 * @author Kevin Ulrich
 */
public class BlobWriter implements DataWriter {

    //logger
    private static Logger logger = Logger.getLogger(BlobWriter.class);

    private BlobOutputStream blobOut;
    private CloudBlockBlob parentBlob;
    private final AccessCondition accessCondition;
    private final BlobRequestOptions options;
    private final OperationContext opContext;

    private final String fileName;


    public BlobWriter(CloudBlockBlob parentBlob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext opContext, String fileName){
        this.parentBlob = parentBlob;
        this.accessCondition = accessCondition;
        this.options = options;
        this.opContext = opContext;
        this.fileName = fileName;
    }

    @Override
    public void open() throws DataAccessObjectInitializationException {
        try{
            blobOut = parentBlob.openOutputStream();
        //}catch(IOException e){
           // String errMsg = Messages.getFormattedString("CSVWriter.errorOpening", this.fileName);
           // throw new DataAccessObjectInitializationException(errMsg, e);
        }catch (StorageException e){
            String errMsg = Messages.getString("BlobWriter.openWriting");
            logger.error(errMsg, e);
        }
    }

    @Override
    public void close() {
        try {
            blobOut.close();
        }catch(IOException e){
            String errMsg = Messages.getString("BlobWriter.closeWriting");
            logger.error(errMsg, e);
        }
    }

    @Override
    public boolean writeRow(Row inputRow) throws DataAccessObjectException {
        try{
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(inputRow);
            blobOut.write(byteOut.toByteArray());
            return true;
        }catch(IOException e){
            String errMsg = Messages.getString("BlobWriter.errorWriting");
            logger.error(errMsg, e);
            throw new DataAccessObjectException(errMsg, e);
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

    }

    @Override
    public List<String> getColumnNames() {
        return null;
    }

    @Override
    public int getCurrentRowNumber() {
        return 0;
    }
}
