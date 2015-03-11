-- Michał Woś
-- Big data processing course, 2015
-- University of Warsaw

%default input 'sample_input'
%default shingle_size 3
-- hash_functions_number == signature size
%default hash_functions_number 100
-- band_size should divide hash_functions_number 
%default band_size 5

register 'shingling/target/shingling-1.0-SNAPSHOT.jar'
register 'minhashing/target/minhashing-1.0-SNAPSHOT.jar'
register 'lsh/target/lsh-1.0-SNAPSHOT.jar'

define dataBagStringConcate DataBagStringConcate();
define shingle Shingler('$shingle_size');
define minhashing Minhashing('$hash_functions_number');
define createBands BandCreator('$band_size');
define createPairs PairsCreator();

set pig.splitCombination false;

-------------------------------------------------------------------------------------------------
-- Prepare data
-------------------------------------------------------------------------------------------------
A = LOAD '$input' USING PigStorage('\t', '-tagFile') AS (docname:chararray, doctext:chararray);
-- we didn't serialize documents (one file = one doc), so now paragraphs are separated by EOL to rows
-- we want to concatenate paragraphs from same documents
B = GROUP A BY docname;
C = FOREACH B GENERATE group AS docname, FLATTEN(dataBagStringConcate(A.doctext)) AS doctext;


-------------------------------------------------------------------------------------------------
-- Shingle
-------------------------------------------------------------------------------------------------
D = FOREACH C GENERATE docname, shingle(doctext) AS docshingle;
-- To see set of shingle for each document, uncomment:
-- DUMP D;

E = FOREACH D GENERATE docname, FLATTEN(docshingle) as shingle; 

-- Prepare sum of shingle
F = FOREACH E GENERATE shingle;
SHINGLE_SUM = DISTINCT F;

SHINGLE_SUM_NUM = FOREACH (GROUP SHINGLE_SUM ALL) {
        GENERATE COUNT(SHINGLE_SUM) as shingle_total_num;
    };

-- sort and add ID to each shingle
SHINGLE_SUM = RANK SHINGLE_SUM by shingle ASC;
SHINGLE_SUM = FOREACH SHINGLE_SUM GENERATE rank_SHINGLE_SUM as shingle_id, shingle;

-- join shingle id with shingle for each document
G = JOIN E by shingle, SHINGLE_SUM by shingle;
H = FOREACH G GENERATE docname, shingle_id;
I = GROUP H by docname;
J = FOREACH I generate group as docname, H.shingle_id as shingle_ids;


-------------------------------------------------------------------------------------------------
-- MinHashing
-------------------------------------------------------------------------------------------------
-- add info about total shingles number to each row 
-- by using dummy value such as 1 we use JOIN as well performed CROSS
-- we use 'replicated' to keep SHINGLE_SUM_NUM (which has 1 row) in memory to preserve nice performance
K = JOIN J by 1, SHINGLE_SUM_NUM by 1 USING 'replicated'; 
SIGNATURES = FOREACH K GENERATE docname as docname, minhashing(shingle_ids, shingle_total_num) as doc_signature;
-- To see minhashing signature for each document, uncomment:
-- DUMP SIGNATURES;

-------------------------------------------------------------------------------------------------
-- LSH
-------------------------------------------------------------------------------------------------
M = FOREACH SIGNATURES GENERATE docname as docname, FLATTEN(createBands(doc_signature)) as (band_signature, band_level);
O = GROUP M by (band_level, band_signature);
BUCKETS = FOREACH O GENERATE M.docname as bucket;
BUCKETS = DISTINCT BUCKETS;
DOC_PAIRS = FOREACH BUCKETS GENERATE FLATTEN(createPairs(bucket));
DOC_PAIRS = DISTINCT DOC_PAIRS;
DUMP DOC_PAIRS;

