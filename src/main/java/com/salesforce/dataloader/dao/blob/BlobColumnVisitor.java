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

import java.util.List;

import com.salesforce.dataloader.model.Row;
import org.apache.log4j.Logger;

/**
 * Describe your class here.
 *
 * @author Lexi Viripaeff
 * @since 6.0
 */
public class BlobColumnVisitor {

/*
An engine to put quotes and commas in the correct places and to concatenate strings.
 */

    private boolean first = true;
    private String concat = "";

    public String visitHeader(List<String> columnNames, boolean capitalization){

        for (String colName : columnNames) {
            String outColName;
            if (colName != null) {
                if (capitalization) {
                    outColName = (colName.toUpperCase());
                } else {
                    outColName = colName;
                }
            } else {
                outColName = "";
            }

            wrap(outColName);
        }

        concat += "\n"; //Insert new line to separate rows
        return concat;
    }

    public String visit(List<String> columnNames, Row inputRow){
        for (String colName : columnNames) {
            Object colVal = inputRow.get(colName);
            colName = (colVal != null ? colVal.toString() : "");
            String colNameEdit = colName.replace("\n", " "); //Removes new lines and replaces with spaces
            wrap(colNameEdit);
        }

        concat += "\n"; //Insert new line to separate rows
        return concat;
    }

    private void wrap(String s){
        if(first){
            concat += s;
            first = false;
        }else{
            concat += ",";
            concat = concat + "\"" + s + "\"";
        }
    }

}
