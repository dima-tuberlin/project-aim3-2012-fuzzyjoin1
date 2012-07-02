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
package cc.clabs.stratosphere.fuzzyjoin.contracts.stage1;

import cc.clabs.stratosphere.fuzzyjoin.types.*;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.StringTokenizer;

/**
 * Splits a record into its tokens and emits each token and 1.
 * Beforehand the record will be converted to lowercase and all
 * non-word characters will be replaced by spaces.
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class TokenizeRecord extends MapStub<PactNull, PactRecord, PactString, PactInteger> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void map( PactNull key, PactRecord record, Collector<PactString, PactInteger> collector ) {
        PactInteger one = new PactInteger( 1 );
        String line = record.getValue().toLowerCase().replaceAll( "\\W", " " );        
        StringTokenizer tokenizer = new StringTokenizer( line );
        while ( tokenizer.hasMoreElements() )
            collector.collect(
                new PactString( (String) tokenizer.nextElement() ),
                one
            );
    }

}