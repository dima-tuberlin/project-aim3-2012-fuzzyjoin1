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
package cc.clabs.stratosphere.fuzzyjoin;

import cc.clabs.stratosphere.fuzzyjoin.contracts.stage1.*;
import cc.clabs.stratosphere.fuzzyjoin.contracts.stage2.*;
import cc.clabs.stratosphere.fuzzyjoin.contracts.stage3.*;
import cc.clabs.stratosphere.fuzzyjoin.io.*;
import cc.clabs.stratosphere.fuzzyjoin.types.*;

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.*;

/**
 * The Great Plan!
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
public class FuzzyPlan implements PlanAssembler, PlanAssemblerDescription {
    
    final static double THRESHOLD = 0.2d;

    public Plan getPlan( String... args ) {
        // all files
        String dbfileR = "file:/home/rob/R.txt";
        String dbfileS = "file:/home/rob/S.txt";
        String outputfile = "file:/home/rob/output.txt";
        
        FileDataSourceContract<PactRecordKey, PactRecord> dataR =
            new FileDataSourceContract<PactRecordKey, PactRecord>
            ( MultisetInputFormat.class, dbfileR, "Dataset R" );
        dataR.setParameter( "DATASET_NAME", "R" );
        
        FileDataSourceContract<PactRecordKey, PactRecord> dataS =
            new FileDataSourceContract<PactRecordKey, PactRecord>
            ( MultisetInputFormat.class, dbfileS, "Dataset S" );
        dataS.setParameter( "DATASET_NAME", "S" );
        
        /**     _                   _
         *  ___| |_ __ _  __ _  ___/ |
         * / __| __/ _` |/ _` |/ _ \ |
         * \__ \ || (_| | (_| |  __/ |
         * |___/\__\__,_|\__, |\___|_|
         *               |___/
         */
        MapContract<PactRecordKey, PactRecord, PactString, PactInteger> tokenizer =
            new MapContract<PactRecordKey, PactRecord, PactString, PactInteger>
            ( TokenizeRecord.class, "Tokenize Dataset R" );
        
        ReduceContract<PactString, PactInteger, PactInteger, PactString> counter =
            new ReduceContract<PactString, PactInteger, PactInteger, PactString>
            ( CountTokens.class, "Sum up token frequencies" );

        ReduceContract<PactInteger, PactString, PactNull, PactTokenlist> packer =
            new ReduceContract<PactInteger, PactString, PactNull, PactTokenlist>
            ( PackTokenlist.class, "Packing Tokens" );
        
        ReduceContract<PactNull, PactTokenlist, PactNull, PactTokenlist> tokens =
            new ReduceContract<PactNull, PactTokenlist, PactNull, PactTokenlist>
            ( EmitTokenlist.class, "Emiting Tokens" );
        tokens.setDegreeOfParallelism( 1 );

        // putting stage1 together
        tokenizer.setInput( dataR );
        counter.setInput( tokenizer );
        packer.setInput( counter );
        tokens.setInput( packer );
        
        /**     _                   ____
         *  ___| |_ __ _  __ _  ___|___ \
         * / __| __/ _` |/ _` |/ _ \ __) |
         * \__ \ || (_| | (_| |  __// __/
         * |___/\__\__,_|\__, |\___|_____|
         *               |___/
         */      
        CrossContract<PactRecordKey, PactRecord, PactNull, PactTokenlist, PactString, PactRecord> emitterR =
            new CrossContract<PactRecordKey, PactRecord, PactNull, PactTokenlist, PactString, PactRecord>
            ( EmitCandidates.class, "Emit Candidates from R" );
        emitterR.setParameter( "THRESHOLD", Double.toString( THRESHOLD ));

        CrossContract<PactRecordKey, PactRecord, PactNull, PactTokenlist, PactString, PactRecord> emitterS =
            new CrossContract<PactRecordKey, PactRecord, PactNull, PactTokenlist, PactString, PactRecord>
            ( EmitCandidates.class, "Emit Candidates from S" );
        emitterS.setParameter( "THRESHOLD", Double.toString( THRESHOLD ));

        CoGroupContract<PactString, PactRecord, PactRecord, PactRecordAndRecord, PactNull> joiner =
            new CoGroupContract<PactString, PactRecord, PactRecord, PactRecordAndRecord, PactNull>
            ( JoinCandidatePairs.class, "Join distinct candidate pairs" );        
        
        ReduceContract<PactRecordAndRecord, PactNull, PactRecordKey, PactRecordJoinKeysAndSimilarity> validator =
            new ReduceContract<PactRecordAndRecord, PactNull, PactRecordKey, PactRecordJoinKeysAndSimilarity>
            ( FilterAndValidateCandidates.class, "Filter and validate candidates" );
        validator.setParameter( "THRESHOLD", Double.toString( THRESHOLD ));
        
        // putting stage 2 together
        emitterR.setFirstInput( dataR );
        emitterR.setSecondInput( tokens );
        emitterS.setFirstInput( dataS );
        emitterS.setSecondInput( tokens );
        joiner.setFirstInput( emitterR );
        joiner.setSecondInput( emitterS );
        validator.setInput( joiner );
        
        /**     _                   _____
         *  ___| |_ __ _  __ _  ___|___ /
         * / __| __/ _` |/ _` |/ _ \ |_ \
         * \__ \ || (_| | (_| |  __/___) |
         * |___/\__\__,_|\__, |\___|____/
         *               |___/
         */

        // java is sooo beautiful and expressive! >_<
        MatchContract<PactRecordKey, PactRecord, PactRecordJoinKeysAndSimilarity, PactRecordJoinKey, PactRecordAndDouble> emitterR3 =
            new MatchContract<PactRecordKey, PactRecord, PactRecordJoinKeysAndSimilarity, PactRecordJoinKey, PactRecordAndDouble>
            ( EmitJoinRecords.class, "Emit join records from R" );

        MatchContract<PactRecordKey, PactRecord, PactRecordJoinKeysAndSimilarity, PactRecordJoinKey, PactRecordAndDouble> emitterS3 =
            new MatchContract<PactRecordKey, PactRecord, PactRecordJoinKeysAndSimilarity, PactRecordJoinKey, PactRecordAndDouble>
            ( EmitJoinRecords.class, "Emit join records from S" );

        MatchContract<PactRecordJoinKey, PactRecordAndDouble, PactRecordAndDouble, PactDouble, PactRecordAndRecord> merger =
            new MatchContract<PactRecordJoinKey, PactRecordAndDouble, PactRecordAndDouble, PactDouble, PactRecordAndRecord>
            ( MergeJoinRecords.class, "Merge join records together");
        
        // putting stage 3 together
        emitterR3.setFirstInput( dataR );
        emitterR3.setSecondInput( validator );
        emitterS3.setFirstInput( dataS );
        emitterS3.setSecondInput( validator );
        merger.setFirstInput( emitterR3 );
        merger.setSecondInput( emitterS3 );

        // finally …
        FileDataSinkContract<PactDouble, PactRecordAndRecord> out =
            new FileDataSinkContract<PactDouble, PactRecordAndRecord>
            ( ResultOutputFormat.class, outputfile, "Write result to file" );
        out.setInput( merger );
        
        // Yay-ho!
        return new Plan( out, "Fuzzyjoin");
    }

    /**
    * {@inheritDoc}
    */
    public String getDescription() {
        return "Parameters: none so far … ;)";
    }
    
}