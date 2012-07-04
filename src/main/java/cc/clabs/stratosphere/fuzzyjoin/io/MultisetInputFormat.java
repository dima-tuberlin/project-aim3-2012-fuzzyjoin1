/**
 *  ______ __  __  ______  ______  __  __     __  ______  __  __   __
 * /\  ___/\ \/\ \/\___  \/\___  \/\ \_\ \   /\ \/\  __ \/\ \/\ "-.\ \
 * \ \  __\ \ \_\ \/_/  /_\/_/  /_\ \____ \ _\_\ \ \ \/\ \ \ \ \ \-.  \
 *  \ \_\  \ \_____\/\_____\/\_____\/\_____/\_____\ \_____\ \_\ \_\\"\_\
 *   \/_/   \/_____/\/_____/\/_____/\/_____\/_____/\/_____/\/_/\/_/ \/_/
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob@CLABS.CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package cc.clabs.stratosphere.fuzzyjoin.io;

import cc.clabs.stratosphere.fuzzyjoin.types.*;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Basic input format for datasets. Each row begins with a record id
 * followed by a tab and the record itself.
 * 
 * FORMAT: "RID \t RECORD"
 * 
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
@UniqueKey
public class MultisetInputFormat extends TextInputFormat<PactRecordKey, PactRecord> {
    
    private String DATASET_NAME;
    
    @Override
    public void configure( Configuration parameters ) {
        super.configure( parameters );
        DATASET_NAME = parameters.getString( "DATASET_NAME", this.filePath.toString() );
    }
    
    @Override
    public boolean readLine( KeyValuePair<PactRecordKey, PactRecord> pair, byte[] line ) {
        // split line
        String[] splits = new String( line ).split( "\t", 2 );
        if ( splits.length != 2 ) return false;
        // instanciate objects
        PactRecordKey key = new PactRecordKey( DATASET_NAME, Integer.parseInt( splits[ 0 ] ) );
        PactRecord record = new PactRecord( key, new PactString( splits[ 1 ] ) );
        // set Key/Value
        pair.setKey( key );
        pair.setValue( record );
        return true;
    }
}