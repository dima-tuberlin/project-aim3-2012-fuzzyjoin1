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

import cc.clabs.stratosphere.fuzzyjoin.types.PactTokenlist;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Packs all tokens with the same frequency together in a Tokenlist.
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PackTokenlist extends ReduceStub<PactInteger, PactString, PactNull, PactTokenlist> {
    
    @Override
    public void reduce( PactInteger key, Iterator<PactString> tokens, Collector<PactNull, PactTokenlist> collector ) {
        ArrayList<String> list = new ArrayList<String>();
        while ( tokens.hasNext() )
            list.add( tokens.next().getValue() );
        collector.collect( new PactNull(), new PactTokenlist( list ));        
    }

}